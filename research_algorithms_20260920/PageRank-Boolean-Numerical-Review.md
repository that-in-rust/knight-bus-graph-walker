# Boolean PageRank Numerical Review A01

Date: 2026-09-21. Independent mathematical, numerical and solver-code review.
Only this new file is owned. No code, test, other-document edits or commits.
The user goal of seven paper-worthy algorithms remains open.

**Lead resolution, after this frozen review:** the P1 below was reproduced
and repaired using the proposed centered action. The [implementation evidence](PageRank-Boolean-Streaming-Evidence.md)
records the failing regression, 61 passing combined tests, post-repair replay
of this checker, and new measurements. The original findings/checker/hash are
retained as historical evidence. Reproduction against the fixed solver requires
rebinding the hash and replacing the scalar witness's expected P error by zero;
the lead performed exactly those two changes in memory, not to this checker.
Post-repair solver SHA-256:
`aaa467f43372d286e2f9a3d14538706e93591d469019710d8cdc1d55a2d6ae5e`.

## Findings First

### P1: The P Comparator Can Converge To The Wrong Operator

**Actionable before comparative claims.** In
[stream_boolean_rank_solver.py](experiments/stream_boolean_rank_solver.py),
lines 194-200, the P matvec evaluates
`h*(t*q - alpha*sum(group_totals))`. This cancels the small ground against
large class-self terms. The correctly assembled Jacobi diagonal at lines
183-185 does not repair the inaccurately evaluated operator.

Witness: two distinct vertices, both with singleton signature `{0}`,
weights `(1,1)`, `alpha=math.nextafter(1.0,0.0)`, default tolerance.
Both original degrees are one. Exact PageRank is `(1/2,1/2)`.

| Plan | Emitted values | Actual L1 error | `converged` | Recomputed relative residual |
| --- | --- | --- | --- | --- |
| F CG | `(0.5,0.5)` | 0 | true | 0 |
| P CG | `(0.25,0.25)` | 0.5 | true | 0 |

Writing `epsilon=1-alpha=2^-53`, the exact scalar class matrix is
`K=[2*epsilon]`, with condition number **one**. The first preconditioned
direction is `q=1/2`. Binary64 rounds `t=1+alpha` to 2, so the evaluated
matvec is `2*(1-alpha)=2*epsilon`, twice its exact value `epsilon`.
CG halves the solution. Re-evaluating the same rounded matvec returns
the same wrong equation and zero residual. This is not intrinsic scalar
ill-conditioning, a failed SPD derivation, or evidence that F elimination
is intrinsically more accurate than a competent P solver.

The exact original-operator residual bound for the wrong output is 0.5,
not zero. A correct publication certificate should reject it at tight
absolute tolerance. This review did not inspect or test the separate
actual-output certifier, and makes no claim of a publication bypass.

**Requested correction:** use the bounded centered evaluation below,
retain the Jacobi diagonal, add this regression, and repeat comparisons
with the corrected control. Do not promote the current P failures as
algorithmic differentiation.

### Range And Flag Limits: Disclosed, Not False Certificates

At solver lines 49-57 and 65-66 the norm is scaled, but CG dot products
are not. An isolate weighted 1 plus two adjacent singleton members weighted
`2^-540` each makes both plans raise `ArithmeticError` on the first step.
Products in `r^T D^-1 r` and/or `p^T A p` underflow despite nonzero normal
RHS entries. This is an avoidable range limitation, but numerical refusal
is explicitly allowed by the stated contract.

A robustness follow-up can power-of-two scale the RHS and unscale the
solution, and/or use scaled dot products. One global scale cannot fix
every disconnected-component dynamic range. Check the original output;
do not silently convert breakdown into successful convergence.

The reverse flag mismatch is also real. Nine high-alpha F solves below
all report false flags; their largest actual L1 error is
`4.55376477267085e-15`. Neither true nor false numerical flags decide
the original absolute-L1 contract. Certification may still refuse an
accurate answer if its rigorous residual bound is too loose.

## Mathematical Assessment

### P System And SPD

Let `V` copy each active class coordinate `q_H` to its `h_H` vertices.
The original active matrix `H0=D-alpha*A` is SPD since

~~~text
v^T H0 v = (1-alpha)*sum_i d_i*v_i^2
           + alpha*sum_{unordered edges}(v_i-v_j)^2 > 0.
~~~

`V` has full column rank, so `K=V^T H0 V` is SPD. Its entries are

- `K_HH=h_H*(d_H-alpha*(h_H-1))`.
- `K_HG=-alpha*h_H*h_G` for distinct intersecting signatures.
- `K_HG=0` for distinct disjoint signatures.

Distinct signatures intersect at most once under the two-membership
restriction. Substitution of `S_a=sum_(H incident a) h_H*q_H` gives

`h_H*t_H*q_H-alpha*h_H*u_H^T S=B_H`.

The implemented diagonal
`h_H*((1-alpha)*(h_H-1)+(d_H-h_H+1))` is correct and uses nonnegative
terms, since `d_H>=h_H-1`. `B_H` already includes the isolate adjustment;
no extra `h_H` belongs on the RHS.

Arbitrary personalization is recovered by resolving within-class
zero-sum contrasts. The implemented local lift is equivalent in exact
arithmetic; dividing every class mass equally would not be correct.

### Grounded F And Jacobi Equivalence

For active counts `N`, `R=diag(N)` and `C=I-M`, the grounded system is
`L=R C R`, `S=R y`, `L y=R c`. Its positive ground/edge coefficients
match a separately expanded-vertex block elimination in the checker.
The implementation does not materialize a dense F matrix.

Writing `D_C=diag(C)` and `D_L=diag(L)=R^2 D_C` yields

`D_L^(-1/2) L D_L^(-1/2)=D_C^(-1/2) C D_C^(-1/2)`.

Thus Jacobi-preconditioned L and C have the **same symmetric spectrum**;
their exact PCG iterates correspond under `S=R y` with corresponding
initial states/RHS. The checker verifies exact diagonal similarity and
numerical solve equivalence.

The valid unpreconditioned bound
`kappa_2(L)<=(N_max/N_min)^2*kappa_2(C)` does not describe that Jacobi
spectrum: Jacobi removes the count congruence. Do not transfer
`kappa_2(C)<=(1+3*alpha)/(1-alpha)` directly to raw L or the P matrix.

A conservative exact-arithmetic Jacobi bound follows from the
manuscript's weighted contraction: its normalized spectrum lies in
`[1-beta,1+beta]`, hence

`kappa_J<=(1+beta)/(1-beta)<=(1+7*alpha)/(1-alpha)`.

These are upper bounds, not measured iteration counts, binary64 accuracy
guarantees or speed claims. The congruence is on active factors.
Inactive supplied factors add identity equations with zero RHS and still
consume allocated raw-F slots.

### Isolates And Stable Lifting

The exact `gamma=(1-alpha)/(1-alpha*p_Z)` is correct for this undirected
graph, including all isolates and personalization solely on isolates.
Empty input and alpha zero require no linear solve.

The local expression at solver lines 218-231 is correct:

`x_v=d_H*(b_v+alpha*(sum_(a in H) S_a-(k_H-1)*Q_H))/(d_H+alpha)`.

At a genuine nonnegative pair-class solution,
`sum S=2*Q_H+E_H` with `E_H>=0`, so subtracting one `Q_H` has cancellation
condition at most 3. Approximate states need not satisfy that property.
Negative/nonfinite output rejection is appropriate. Unequal preferences
on a two-vertex pair at alpha `2^-60` retain the tiny positive score.

This avoids mean-minus-contrast cancellation; it does not guarantee
relative accuracy for every subnormal. RHS rounding, dot products,
and the lift's division followed by multiplication remain finite-range
operations. Complete output still needs independent checking.

## Bounded P Evaluation Repair

An exact positive-ground/difference action is

~~~text
(K q)_H = (1-alpha)*h_H*d_H*q_H
          + alpha*h_H*sum_(G != H, intersects H) h_G*(q_H-q_G).
~~~

No class-pair adjacency expansion is needed. For each group `a`, choose
`c_a` as its first incident class value in fixed stream order, and gather

~~~text
N_a = sum_(G incident a) h_G
T_a = sum_(G incident a) h_G*(q_G-c_a).
~~~

The second class pass computes

~~~text
(K q)_H = (1-alpha)*h_H*d_H*q_H
          + alpha*h_H*sum_(a in H) [N_a*(q_H-c_a)-T_a].
~~~

Self terms are zero, even for a pair class. Every other neighboring
signature occurs exactly once. This is the same exact SPD operator
with the same diagonal. On a constant class vector, all difference
terms vanish exactly in floating arithmetic: the small ground is no
longer extracted by subtracting class-self terms.

Cost remains two class passes, P input/output coordinates, and O(raw F)
counts, centers, centered totals and compensation/initialization state.
Charge those arrays and recompute payload accounting when implementing.

The independent in-memory variant below matches exact
basis/signed/constant matvecs and fixes the scalar witness with zero
actual error. This is bounded evidence, not a universal floating-SPD,
positivity, convergence or accuracy proof. Signed nonconstant vectors
can still incur cancellation. No production solver was modified.

## Contract And Resource Checks

| Item | Result / necessary qualification |
| --- | --- |
| Raw F | Allocated supplied F includes inactive groups. One fixture has raw F=9 and two active factors. Use raw F, not the manuscript's active F, in implementation payload claims. |
| P versus F | Six pair signatures on four groups give P>F; one repeated pair gives P<F. F has no retained P iterate; P also needs F group gathers/output state. |
| Integer refusal | Both plans reject tested h or degree `2^53+1`; F also rejects a group count `2^53+1`. These checks do not prove metadata agrees with graph semantics. |
| Fast paths | Empty/alpha-zero paths skip coefficient scans. State refusal as applying to coefficients used, or enforce a stronger source contract separately. |
| Reservations | Tested invalid budgets reject before source scans/allocation. Slot limits are not byte/RSS caps. |
| Packed payload | `8*(12*dimension+6*raw_F)` is conservatively consistent with reviewed packed vectors. It excludes scalar Fractions/integers, objects, source/cache residency, Python/runtime/OS, output and certifier state. |
| Pass accounting | F matvec: one filtered class scan; P: two plus final group gathering. Source scans include inactive records. Output scans are outside solve counters. Count passes, not just CG iterations. |
| Weight range | Two maximum finite weights, whose sum exceeds binary64 range, normalize correctly through Fraction. Two minimum subnormal weights also normalize/solve correctly. This is not exhaustive exponent coverage. |
| Snapshot binding | A different token rejects output before rows are yielded. Equality assumes immutable source/content/query binding; solver does not authenticate source contents independently. |
| Output order | Class then source member order, not globally ID sorted. The provider contract supplies within-class ID order. Exhaust the generator to execute its terminal universe-count check. |
| Flags | `converged` uses recomputed numerical relative residual up to `10*tolerance`, not original absolute L1 error. |
| Control fairness | Resident P is only one control. File-backed P, paid preparation, joins, passes, complete output and certification remain necessary later comparisons. |

Source preparation/eligibility and actual-output certificate internals
were not inspected. Old native interval code was not needed. The lead's
subsequently reported signed-ID and exception/cursor fixes are separate
integration work, not independently audited findings here.

## Evidence

The eight existing lead tests pass. Their seeded test comprises 100
layouts times three alphas times two methods = 600 seeded solves, not
600 independent datasets.

This checker uses its own source fixture, explicit Boolean adjacency
for degrees, and an independent rational original stochastic-system
solve including dangling redistribution. It imports neither the lead
fixture nor its oracle.

- 64 differently seeded layouts plus five explicit cases, four alphas
  and two plans: **552 ordinary solves**. Maximum actual L1 error:
  `6.704906790411283e-13`.
- 21 operator cases: exact SPD pivots, expanded F elimination, P
  congruence/diagonal, Jacobi similarity/equivalence, captured production
  actions and proposed centered actions.
- Scalar high-alpha defect/correction, and 18 extra high-alpha solves.
  Largest uncorrected P error in the latter: `0.7753906249999325`.
- Exact original residual/error-bound checks on every completed solve.
  These are resident value checks, **not output-byte certification**.
- Tiny-alpha contrasts, isolates, zero-active preference, dimensions,
  zero iterations, snapshot mismatch, integer/slot refusal, normalization
  extremes and dot-product underflow refusal.

No performance benchmarks, physical-memory measurements, broad
literature review or priority search were performed.

### Source Snapshot

Initial numerical exploration used solver hash
`e6661aa34b030e314aec7e35cbc987d2052b386c47eb67140e14383eddd5f928`.
During report preparation the lead added iterator-lifetime handling. The
hash guard detected the change; the numerical formulas remained unchanged.
The checker was rebound to the following snapshot and rerun, with the same
numerical results. Line numbers refer to this second snapshot. Later source
changes deliberately require an explicit recheck.

| File | SHA-256 |
| --- | --- |
| `experiments/stream_boolean_rank_solver.py` | `889aa3f97c2a6ee9d247e9c35166eed5da87f8f37808207be67b379864000e81` |
| `experiments/test_stream_boolean_rank_solver.py` | `9f2d03208c9077502e249c070d4179069f4f4982d8699353b68076974c3a0833` |
| `PageRank-Boolean-Two-Membership.md` | `de22bd745c4bc356206917f9016eab6a977fa64bd8cdff223f250e6fd67df761` |
| `PageRank-Boolean-Streaming-Evidence.md` | `359fbf915cfcfd426e22473e1b065130b2dbd5c76f903d1a0d0af03267e144ff` |

### Reproduction

Run at repository root. Commands avoid bytecode writes. Resident/dense
checker fixtures establish no solver memory bound.

~~~sh
python3 -B -m unittest discover -s research_algorithms_20260920/experiments -p test_stream_boolean_rank_solver.py -v
awk '/^```python$/{block=1;next} /^```$/{if(block)exit} block' research_algorithms_20260920/PageRank-Boolean-Numerical-Review.md | python3 -B
~~~

Python 3.9.6. Lead tests: 8 passed. Independent checker stdout:

~~~text
ordinary exact-oracle solves: 552 maximum L1: 6.704906790411283e-13
exact SPD / grounded / class / Jacobi operator cases: 21
high-alpha factor-cg values [0.5, 0.5] L1 0.0 converged True recomputed 0.0
high-alpha class-cg values [0.25, 0.25] L1 0.5 converged True recomputed 0.0
centered comparator scalar witness: L1=0
high-alpha extra solves: 18; F false flags: 9 F max L1: 4.55376477267085e-15 P max L1: 0.7753906249999325
subnormal dot products: both plans refuse at active weight 2^-540
exact normalization: two maximum finite weights and two minimum subnormals pass
tiny-alpha contrast, zero-step screen, snapshot, reservations, integer refusals: pass
reviewed solver SHA256: 889aa3f97c2a6ee9d247e9c35166eed5da87f8f37808207be67b379864000e81
~~~

## Independent Checker

```python
from array import array
from fractions import Fraction as Q
from itertools import combinations
from pathlib import Path
import hashlib
import math
import random
import sys

ROOT = Path("research_algorithms_20260920")
PATH = ROOT / "experiments/stream_boolean_rank_solver.py"
EXPECTED = "889aa3f97c2a6ee9d247e9c35166eed5da87f8f37808207be67b379864000e81"
actual = hashlib.sha256(PATH.read_bytes()).hexdigest()
assert actual == EXPECTED, ("review snapshot changed", actual)
sys.path.insert(0, str(PATH.parent))
import stream_boolean_rank_solver as solver


def solve_independent_exact_system(matrix, rhs):
    n = len(rhs)
    work = [[Q(value) for value in row] + [Q(rhs[i])]
            for i, row in enumerate(matrix)]
    for j in range(n):
        pivot = next(i for i in range(j, n) if work[i][j])
        work[j], work[pivot] = work[pivot], work[j]
        denominator = work[j][j]
        work[j] = [value / denominator for value in work[j]]
        for i in range(n):
            if i != j:
                multiplier = work[i][j]
                work[i] = [a - multiplier * b for a, b in zip(work[i], work[j])]
    return [row[-1] for row in work]


class IndependentNativeSourceFixture:
    def __init__(self, rows, factors):
        self.rows = [(v, frozenset(g), float(w)) for v, g, w in rows]
        self.factor_count, self.vertex_count = factors, len(rows)
        self.snapshot_id = repr((self.rows, factors))
        self.calls = 0
        self.adj = [[int(i != j and bool(a[1] & b[1]))
                     for j, b in enumerate(self.rows)] for i, a in enumerate(self.rows)]
        self.degrees = list(map(sum, self.adj))
        self.total_weight = sum((Q(w) for _, _, w in self.rows), Q())
        self.isolate_weight = sum((Q(row[2]) for row, d in zip(self.rows, self.degrees) if not d), Q())
        signatures = sorted(set(tuple(sorted(g)) for _, g, _ in self.rows))
        self.records, self.members = [], {}
        self.counts = [0] * factors
        for cid, signature in enumerate(signatures):
            indices = [i for i, row in enumerate(self.rows) if row[1] == set(signature)]
            self.members[cid] = indices
            d = self.degrees[indices[0]]
            assert all(self.degrees[i] == d for i in indices)
            self.records.append((cid, signature, len(indices), d,
                                 sum((Q(self.rows[i][2]) for i in indices), Q())))
            if d:
                for a in signature:
                    self.counts[a] += len(indices)
        self.active_class_count = sum(d > 0 for _, _, _, d, _ in self.records)

    def iterate_class_records(self):
        self.calls += 1
        return iter(self.records)

    def iterate_active_factor_counts(self):
        self.calls += 1
        return iter(enumerate(self.counts))

    def iterate_class_vertex_rows(self, cid):
        self.calls += 1
        return iter(sorted((self.rows[i][0], self.rows[i][2]) for i in self.members[cid]))


def compute_independent_exact_answer(source, alpha):
    n, a = source.vertex_count, Q(alpha)
    if not n:
        return {}
    p = [Q(row[2]) / source.total_weight for row in source.rows]
    operator = [[Q(i == j) - a * (Q(source.adj[i][j], source.degrees[j])
                 if source.degrees[j] else p[i]) for j in range(n)] for i in range(n)]
    answer = solve_independent_exact_system(operator, [(1-a)*value for value in p])
    return {row[0]: value for row, value in zip(source.rows, answer)}


def compute_original_residual_bound(source, alpha, values):
    if not source.vertex_count:
        return Q()
    a = Q(alpha)
    x = [Q(values[row[0]]) for row in source.rows]
    p = [Q(row[2])/source.total_weight for row in source.rows]
    dangling = sum((value for value, d in zip(x, source.degrees) if not d), Q())
    residual = [(1-a)*p[i] + a*(p[i]*dangling +
                 sum((Q(source.adj[i][j], d)*x[j] for j,d in enumerate(source.degrees) if d), Q())) - x[i]
                for i in range(source.vertex_count)]
    return sum(map(abs,residual), Q())/(1-a)


def run_independent_solver_case(source, alpha, method, **options):
    args = dict(alpha=alpha, method=method, max_factor_slots=source.factor_count,
                max_class_slots=source.active_class_count, tolerance=1e-13, maximum_iterations=1000)
    args.update(options)
    state = solver.solve_boolean_rank_state(source, **args)
    values = dict(solver.iterate_boolean_rank_output(source, state))
    expected = compute_independent_exact_answer(source, alpha)
    assert set(values) == set(expected)
    error = sum((abs(Q(values[v]) - expected[v]) for v in expected), Q())
    assert error <= compute_original_residual_bound(source, alpha, values)
    return state, values, error


def require_independent_expected_refusal(call, exception):
    try:
        call()
    except exception:
        return
    raise AssertionError("expected refusal")


def check_exact_positive_pivots(matrix):
    work = [row[:] for row in matrix]
    for j in range(len(work)):
        pivot = work[j][j]
        assert pivot > 0
        for i in range(j + 1, len(work)):
            for k in range(j + 1, len(work)):
                work[i][k] -= work[i][j] * work[j][k] / pivot


def assemble_independent_exact_operators(source, alpha):
    a, f = Q(alpha), source.factor_count
    active = [i for i, d in enumerate(source.degrees) if d]
    records = [r for r in source.records if r[3]]
    hessian = [[Q(source.degrees[i] if i == j else 0) - a*source.adj[i][j]
                for j in active] for i in active]
    incidence = [[Q(group in source.rows[i][1]) for group in range(f)] for i in active]
    block = [[hessian[i][j] + a*sum(incidence[i][k]*incidence[j][k] for k in range(f))
              for j in range(len(active))] for i in range(len(active))]
    inverse_columns = [solve_independent_exact_system(block, [row[k] for row in incidence])
                       for k in range(f)]
    core = [[Q(i == j) - a*sum(incidence[v][i]*inverse_columns[j][v]
                              for v in range(len(active))) for j in range(f)] for i in range(f)]
    # Inactive raw factors get identity equations; do not multiply them by zero.
    scale = [Q(n or 1) for n in source.counts]
    grounded = [[scale[i]*core[i][j]*scale[j] for j in range(f)] for i in range(f)]
    quotient = [[sum((Q(source.degrees[u] if u == v else 0) - a*source.adj[u][v]
                      for u in source.members[left[0]] for v in source.members[right[0]]), Q())
                 for right in records] for left in records]
    for matrix in (core, grounded, quotient):
        assert matrix == list(map(list, zip(*matrix))) if matrix else True
        check_exact_positive_pivots(matrix)
    for i in range(f):
        for j in range(f):
            assert grounded[i][j]/grounded[i][i] == core[i][j]/core[i][i]*scale[j]/scale[i]
    for i, (_, _, h, d, _) in enumerate(records):
        assert quotient[i][i] == h*((1-a)*(h-1) + d-h+1)
    return core, grounded, quotient


def apply_centered_class_operator(source, alpha, values):
    # Independent proposed two-pass evaluation; not a production edit.
    records = [r for r in source.records if r[3]]
    f = source.factor_count
    centers, totals, errors, counts = [None]*f, [0.0]*f, [0.0]*f, [0]*f
    for value, (_, groups, h, _, _) in zip(values, records):
        for group in groups:
            if centers[group] is None:
                centers[group] = value
            increment = h*(value-centers[group]) - errors[group]
            new = totals[group] + increment
            errors[group] = (new-totals[group])-increment
            totals[group] = new
            counts[group] += h
    result = []
    for value, (_, groups, h, d, _) in zip(values, records):
        differences = math.fsum(counts[g]*(value-centers[g])-totals[g] for g in groups)
        result.append((1-alpha)*h*d*value + alpha*h*differences)
    return array("d", result)


def check_captured_operator_actions(source, alpha):
    core, grounded, quotient = assemble_independent_exact_operators(source, alpha)
    original = solver.solve_preconditioned_conjugate_gradient
    for method, expected in (("factor-cg", grounded), ("class-cg", quotient)):
        def inspect(apply, rhs, diagonal, tolerance, maximum_iterations):
            n = len(rhs)
            probes = [[float(i == j) for i in range(n)] for j in range(n)]
            probes += [[(-1.0)**i*(i+1)/7 for i in range(n)], [0.5]*n]
            for probe in probes:
                truth = [float(sum((row[i]*Q(probe[i]) for i in range(n)), Q())) for row in expected]
                computed = apply(probe)
                assert all(abs(x-y) <= 2e-12*max(1., abs(y)) for x, y in zip(computed, truth))
                if method == "class-cg":
                    centered = apply_centered_class_operator(source, alpha, probe)
                    assert all(abs(x-y) <= 2e-12*max(1., abs(y)) for x, y in zip(centered, truth))
            assert all(abs(x-float(expected[i][i])) <= 2e-12*max(1., abs(x))
                       for i, x in enumerate(diagonal))
            return array("d", [0.0])*n, dict(iterations=0, converged=False)
        solver.solve_preconditioned_conjugate_gradient = inspect
        try:
            solver.solve_boolean_rank_state(source, alpha=alpha, method=method,
                max_factor_slots=source.factor_count, max_class_slots=source.active_class_count)
        finally:
            solver.solve_preconditioned_conjugate_gradient = original
    # Operational Jacobi-CG equivalence under L=R C R, not only diagonal algebra.
    f = source.factor_count
    if f:
        scale = [n or 1 for n in source.counts]
        c = [1.0 if n else 0.0 for n in source.counts]
        def action(matrix):
            return lambda x: array("d", (math.fsum(float(v)*w for v, w in zip(row, x)) for row in matrix))
        x, _ = original(action(core), c, [float(core[i][i]) for i in range(f)], 1e-12, 100)
        y, _ = original(action(grounded), [scale[i]*c[i] for i in range(f)],
                        [float(grounded[i][i]) for i in range(f)], 1e-12, 100)
        assert all(abs(x[i]-scale[i]*y[i]) <= 2e-10*max(1., abs(x[i])) for i in range(f))


rng = random.Random(901337)
layouts = []
for _ in range(64):
    f, n = rng.randrange(7), rng.randrange(1, 9)
    rows = [(1009+37*i, rng.sample(range(f), rng.randrange(min(2, f)+1)),
             rng.choice([0., .125, 1., 7.])) for i in range(n)]
    rows[0] = (rows[0][0], rows[0][1], 1.)
    layouts.append(IndependentNativeSourceFixture(rows, f))
layouts += [
    IndependentNativeSourceFixture([], 5),
    IndependentNativeSourceFixture([(1,(0,0,1),9.),(2,(1,0),1.),(3,(0,),2.),(4,(1,),3.),(5,(),4.)], 9),
    IndependentNativeSourceFixture([(1,(0,),0.),(2,(0,),0.),(3,(),1.)], 7),
    IndependentNativeSourceFixture([(1,(),1.),(2,(2,3),3.),(3,(5,),2.)], 8),
    IndependentNativeSourceFixture([(10+i,(a,b),float(i+1)) for i,(a,b) in enumerate(combinations(range(4),2))],4)
]
solves, maximum_error = 0, Q()
for source in layouts:
    for alpha in (0.0, .25, .85, .999):
        for method in ("factor-cg","class-cg"):
            state, values, error = run_independent_solver_case(source, alpha, method)
            assert error < Q(1,10**9), (source.rows, alpha, method, float(error))
            assert len(state["factor_scores"]) == source.factor_count
            assert state["metrics"]["dimension"] == (source.factor_count if method=="factor-cg" else source.active_class_count)
            assert (state["class_scores"] is None) == (method=="factor-cg")
            assert all(math.isfinite(x) and x >= 0 for x in values.values())
            maximum_error = max(maximum_error,error)
            solves += 1
for source in layouts[:16] + layouts[-5:]:
    check_captured_operator_actions(source, .5)
print("ordinary exact-oracle solves:", solves, "maximum L1:", float(maximum_error))
print("exact SPD / grounded / class / Jacobi operator cases:", 21)

pair = IndependentNativeSourceFixture([(40,(0,),1.),(80,(0,),1.)], 1)
alpha = math.nextafter(1.0,0.0)
for method in ("factor-cg","class-cg"):
    state, values, error = run_independent_solver_case(pair, alpha, method)
    print("high-alpha", method, "values", list(values.values()), "L1", float(error),
          "converged", state["metrics"]["converged"], "recomputed", state["metrics"]["recomputed_relative_residual"])
    assert error == (0 if method=="factor-cg" else Q(1,2))
    assert state["metrics"]["converged"]
original = solver.solve_preconditioned_conjugate_gradient
def use_centered_class_matvec(apply, rhs, diagonal, tolerance, maximum_iterations):
    return original(lambda x: apply_centered_class_operator(pair, alpha, x),
                    rhs, diagonal, tolerance, maximum_iterations)
solver.solve_preconditioned_conjugate_gradient = use_centered_class_matvec
try:
    _, _, error = run_independent_solver_case(pair, alpha, "class-cg")
    assert error == 0
finally:
    solver.solve_preconditioned_conjugate_gradient = original
print("centered comparator scalar witness: L1=0")
difficult = [
    [(10,(0,1),1.),(20,(0,2),0.),(30,(0,3),0.)],
    [(10,(0,1),1.),(20,(1,2),0.),(30,(2,3),0.),(40,(3,4),0.)],
    [(10,(0,1),9.),(20,(0,1),1.),(30,(0,),2.),(40,(1,),3.),(50,(),4.)]
]
factor_false = 0
factor_worst = Q()
class_worst = Q()
for rows in difficult:
    source = IndependentNativeSourceFixture(rows,5)
    for damping in (.999999, 1.-2.**-40, math.nextafter(1.,0.)):
        for method in ("factor-cg","class-cg"):
            state, values, error = run_independent_solver_case(source,damping,method)
            if method=="factor-cg":
                factor_false += not state["metrics"]["converged"]
                factor_worst = max(factor_worst,error)
            else:
                class_worst = max(class_worst,error)
assert factor_false == 9 and factor_worst < Q(1,10**12)
print("high-alpha extra solves: 18; F false flags:", factor_false,
      "F max L1:",float(factor_worst),"P max L1:",float(class_worst))

tiny = IndependentNativeSourceFixture([(1,(),1.),(2,(0,),2.**-540),(3,(0,),2.**-540)],1)
for method in ("factor-cg","class-cg"):
    require_independent_expected_refusal(lambda: run_independent_solver_case(tiny,.85,method), ArithmeticError)
print("subnormal dot products: both plans refuse at active weight 2^-540")

large = float.fromhex("0x1.fffffffffffffp+1023")
for source in (IndependentNativeSourceFixture([(1,(0,),large),(2,(0,),large)],1),
               IndependentNativeSourceFixture([(1,(0,),5e-324),(2,(0,),5e-324)],1)):
    for method in ("factor-cg","class-cg"):
        _, values, error = run_independent_solver_case(source,.5,method)
        assert error < Q(1,10**14)
print("exact normalization: two maximum finite weights and two minimum subnormals pass")

contrast = IndependentNativeSourceFixture([(1,(0,1),1.),(2,(0,1),0.)],2)
for method in ("factor-cg","class-cg"):
    _, values, error = run_independent_solver_case(contrast,2.**-60,method)
    assert values[2] > 0 and abs(values[2]/(2.**-60)-1) < 1e-12
state, _, _ = run_independent_solver_case(pair,.5,"factor-cg",maximum_iterations=0)
assert not state["metrics"]["converged"]
pair.snapshot_id += "-changed"
require_independent_expected_refusal(lambda: list(solver.iterate_boolean_rank_output(pair,state)), ValueError)
pair.snapshot_id = state["snapshot_id"]
for method in ("factor-cg","class-cg"):
    before = pair.calls
    require_independent_expected_refusal(lambda: run_independent_solver_case(pair,.5,method,max_factor_slots=0), ValueError)
    assert pair.calls == before
for field in (2,3):
    bad = IndependentNativeSourceFixture([(1,(0,),1.),(2,(0,),1.)],1)
    row = list(bad.records[0]); row[field] = 2**53+1; bad.records[0] = tuple(row)
    for method in ("factor-cg","class-cg"):
        require_independent_expected_refusal(lambda: run_independent_solver_case(bad,.5,method), ValueError)
bad = IndependentNativeSourceFixture([(1,(0,),1.),(2,(0,),1.)],1)
bad.counts[0] = 2**53+1
require_independent_expected_refusal(lambda: run_independent_solver_case(bad,.5,"factor-cg"), ValueError)
print("tiny-alpha contrast, zero-step screen, snapshot, reservations, integer refusals: pass")
assert hashlib.sha256(PATH.read_bytes()).hexdigest() == EXPECTED
print("reviewed solver SHA256:", EXPECTED)
```

## Contribution Boundary

This supports the exact two-membership Boolean domain composition and
a prepared class-stream F-state solve with arbitrary-personalization
recovery. It identifies no new generic Schur complement, elimination,
quotient, Jacobi, CG or residual theorem. The proposed centered evaluation
is a numerical repair using standard grounding/centering.

Any contribution argument must remain about **prepared streaming domain
composition**, correct Boolean semantics, explicitly paid representations,
and complete certified output. Correct the unfair P evaluation before
using comparisons to support even that narrower argument. Demand,
complete-workflow performance advantage and seven-paper readiness are
not demonstrated here.

# PageRank Factor Conditioned Reservation Planner

Date: 2026-09-20. A01 follow-on. Exact planning for a fixed representation and specified evaluator, not a new PageRank definition, globally optimal solver, or physical-memory benchmark.

## Question And Result

The [mixed reservation frontier](PageRank-Mixed-Reservation-Frontier.md) proves that minimizing surviving factors by a cut need not minimize evaluator buffers. Its two-factor family also defeats every uniform-price cut sweep. Enumerating all 2^k vertex orientations is an obvious exact repair, but k is the number of released vertices, not the intrinsic representation complexity.

**Derived result:** enumerate disappearance witnesses for the g supplied factors that can disappear at all. Each has at most three choices: unconstrained, killed by row masking, or killed by column masking. Every consistent choice yields an interval of feasible row counts and an upper charge for surviving factors. The lower envelope of these intervals is the **exact** minimum surviving-factor count at every row count. Consequently the complete named buffer-reservation portfolio can be optimized in time exponential in g, not k, without retaining every orientation or every branch.

This is a fixed-parameter, representation-specific planner. It can close the earlier admission interval when g is small, even if k is large. It does not make factor discovery cheap or guarantee that g is small on useful customer data. Conditioning on a small set of terms and cardinality optimization are established ideas; the candidate contribution is their explicit connection to the proved positive PageRank schedule and its resource contract.

Expert lenses: nonnegative operators, parameterized optimization, bounded-memory execution, and skeptical prior-art comparison. The conventional vertex enumeration, polynomial cut interval, uniform-price sweep, and factor conditioning are compared, rather than assuming the most elaborate planner is best. Use the cut interval or a pure plan when the exact enumeration's work cannot be admitted.

## Fixed Model

Use the same nonnegative U,V, diagonal Q, damping, selected J and numerical format as the preceding A01 notes. Remove identically zero supplied factors in a paid canonicalization pass first. Every remaining original factor f has nonempty supports

```text
L_f = { i : U_if > 0 }       K_f = { j : V_jf > 0 }.
R subset J                  C = J minus R

factor f disappears iff L_f subset R OR K_f subset C.
```

There are r original factors. If L_f has a member outside J, row killing is impossible. If K_f has a member outside J, column killing is impossible. A factor with both exterior supports always survives; do not branch on it. Let g count factors with at least one legal killing mode. Canonical supports and exterior flags require reading the original memberships; no operation obtains them from just a factor-count header.

All k=|J| restored-star slots stay reserved. Thus the theorem is about the specified reservation even when some actual stars are empty; it does not solve the separate empty-star elimination objective. When the earlier nonempty-star conditions hold, the corresponding factor counts also count nonempty factors.

For ell=|R|, the mixed scalar bill and both specialized pure bills are

```text
B(R)  = 2*r0(R) + 2*r + 3*k + 4*max(ell,k-ell) + 4
B_row = 2*r0(J) + r + 7*k + 4.
B_col = 2*r0(empty) + r + 7*k + 4.
```

The original r in scratch terms does not shrink when r0 shrinks. Planning buffers, scalar widths, IDs, indexes, runtime and output remain additional charges. All orientations preserve the same post-release Q and contraction cap; this optimization changes reservation, not convergence.

## Disappearance-Witness Intervals

For each of the g factors choose one available symbol:

| Choice | Constraint imposed | Surviving-factor charge |
| --- | --- | --- |
| `*` | None | Count this factor as surviving for this branch's upper charge |
| `R` | All of L_f must lie in R | Remove its charge |
| `C` | All of K_f must lie in C | Remove its charge |

The `*` symbol does **not** assert that the factor survives. Making that assertion would add disjunctive constraints and invalidate the simple interval argument.

Let P be the union of supports forced into R, N the union forced into C, and d the number of factors assigned R or C. Reject the branch if P intersects N. Otherwise every free vertex can independently be assigned either side, so the exact feasible cardinality interval is

```text
I = [ |P|, k-|N| ]
c = r-d.
```

For every ell in I, there exists a completion with ell row vertices and r0<=c. For example, use P plus the first ell-|P| free original IDs. This completion may accidentally kill unconstrained factors; that only decreases r0. All surviving-factor prices here are nonnegative.

### Exact Envelope Theorem

Define F(ell)=min{r0(R): |R|=ell}. Then

```text
F(ell) = min { c_branch : branch is consistent and ell in I_branch }.
```

**Proof.** Every listed branch completion is feasible and has r0<=c_branch. Therefore F(ell)<=each listed charge, and F(ell)<=their minimum. Conversely take a minimizing orientation R*. For every factor it kills, choose one actual killing witness, row or column; assign `*` to each surviving factor. This branch is consistent, its interval contains ell, and its charge equals r0(R*)=F(ell). The branch minimum is therefore at most F(ell). Both inequalities establish equality. If both killing modes hold, choosing either is sufficient; no fourth mode is needed.

The proof does not require disjoint, nested, equal-size or uniformly weighted supports. Arbitrary overlaps are handled by the P/N consistency check. It also explains why ignoring an exterior membership, or forbidding unconstrained factors from disappearing, would invalidate the planner.

### Exact Single-Plan Corollary

For a consistent branch let ell_b be the nearest feasible integer to k/2, choosing floor(k/2) when it is feasible and clamping it to I otherwise. Evaluate

```text
charge_b = 2*c + 2*r + 3*k + 4*max(ell_b,k-ell_b) + 4.
```

Keep only the lowest charge and its branch witness, and compare it with B_row and B_col. The result is the exact minimum of all mixed schedules and both specialized pure schedules. Each charge is realizable as an upper reservation; a branch induced by an optimal orientation has no larger factor charge and no worse balanced-buffer term. These facts prove equality, not merely an approximation. At a globally winning branch, the reconstructed completion cannot have a strictly smaller actual bill, since that would contradict optimality of the branch minimum. Independent review identified the omitted all-column specialization; admitting that extra constant candidate does not alter the envelope proof.

This one-plan route does not require an O(k)-entry frontier. For another nonnegative cardinality cost phi(ell), replace the clamp with an exact minimum of phi over I. An arbitrary table may require a separately paid range-minimum index. Identity-dependent costs or variable scalar widths do not reduce to this interval without further information.

## Executable Planning Schedule

```text
stream and validate supports; retain selected masks and exterior flags
admit the branch-count and mask-work upper bounds
initialize the incumbent with the cheaper executable pure specialization
for each disappearance-witness assignment:
    build P and N; reject a conflict
    choose the cheapest feasible cardinality
    calculate the branch upper reservation
    retain only a better candidate and its witness
reconstruct the best orientation and independently recount its factors
release planner workspace before allocating the execution workspace
```

Optional full frontier: insert each `(interval, charge, branch-ID)` into a binary range-minimum tree by range-chmin updates. A final downward propagation gives the minimum at each ell. This uses O(k) tags, not O(3^g) retained intervals. Every tag must retain enough bits for a branch ID; a ternary branch ID needs O(g) bits, not a fixed 64-bit integer for arbitrary g. Reconstruct witnesses by decoding their branch IDs against the fixed mode list.

For machine word width w let H=max(1,ceil(k/w)). Dense selected support masks use O(gH) words. Enumerating branches from scratch gives a conservative

```text
single-plan time   O(Z + 3^g * (g+1)*H + k + r)
single-plan memory O(gH + g + H) words, plus counted IDs/metadata
```

where Z is original membership input work and integer charges fit the declared word model. The mode product is actually product_f(1+number_of_legal_modes_f)<=3^g. There is no exponential retained state. A streamed mixed-radix iterator avoids materializing the product. Reading sparse masks from disk instead of retaining them changes these bounds into potentially repeated I/O and needs a new accounting model.

A full frontier adds O(3^g log(k+1)+k) tag operations and O(k) tags; tag bit width includes g and log r. The selected-membership and row-ID layout construction is still separate and paid. A branch budget can stop enumeration safely with the best feasible upper bound, but **not** a claim of exact optimality. The previous cut lower bound remains valid; failure to find a better branch cannot increase it.

### Two-Factor Counterexample Repaired

For the preceding all-to-all selected family, one global factor always survives and only the J-only factor can disappear. Thus g=1 for every even k>=4. There are exactly three branch choices:

```text
J-only factor forced R  -> all-row cardinality
J-only factor forced C  -> all-column cardinality
J-only factor free      -> balanced cardinality, two surviving bases
```

The third branch obtains 5k+12 mixed scalar slots versus 7k+8 for the specialized all-row plan. At k=1,000, these are 5,012 versus 7,008 slots, about 28.5% fewer reserved scalars. For 8-byte slots the difference is only 15,968 bytes; this example is **not** a headline GB-saving workload. It establishes a missed optimum and its exact recovery with three branches instead of 2^1000 orientations. No exponential vertex oracle is executed at k=1,000.

## Strong Comparators And Primary Precedent

The [preceding primary-source table](PageRank-Anisotropic-Release.md#primary-prior-art-inspection) already identifies established graph-representable objectives and low-rank PageRank elimination. The Cornell PDF was unavailable on this lead's new fetch; its earlier inspected attribution is not upgraded to a new successful fetch.

[Tarlow et al., Fast Exact Inference for Recursive Cardinality Models, Sections 1, 2.2 and 4.1](https://lips.cs.princeton.edu/pdfs/tarlow2012cardinality.pdf) was inspected directly in this pass. It explicitly distinguishes efficient unary-plus-cardinality optimization and nested-subset models. Our overlapping factor-disappearance constraints are not simply the paper's nested model. However, treating cardinality as a tractable subproblem and conditioning away other interactions is conventional. That comparison blocks a claim to have invented generic cardinality inference.

A strong generic competitor can branch on the same disjunctive factor-deletion witnesses and call an ordinary constrained cardinality optimizer. That procedure **matches the theorem here**. This is therefore a concrete exact compiler algorithm and a useful small-parameter result, not yet a defensible stand-alone algorithmic novelty claim. A paper would need to justify the combined representation/contraction/planning/execution contract, show a substantive additional result or measured advantage, and compare against same-factor direct and iterative solvers. It must not rely solely on defeating vertex enumeration or one cut sweep.

## Falsification Plan And Scope

The new probe below checks the envelope at every cardinality against independent enumeration of every vertex orientation for small support systems. It executes the range-update tree rather than storing all branches. It also checks the streamed one-plan result including the cheaper all-row kernel, exterior supports, overlapping constraints, k=0, and reconstruction of every frontier witness from its mixed-radix branch ID. These are combinatorial reservation tests, not new stationary-vector or floating-point tests. The probe's extra per-branch witness checks and per-cardinality reconstruction are verification work, not part of the stated single-plan schedule, which retains a best forced-mask witness and reconstructs it once at the end.

Distinct deliberate mutations must fail: dropping the free branch misses the balanced optimum; treating exterior supports as selected permits impossible eliminations. The large-k two-factor check uses the proved family rather than a hidden exponential oracle. All inputs and verifier arrays in this probe are test-only resident data; the Python runtime does not substantiate the packed memory formula.

## Retained New Probe

```python
from itertools import product
from random import Random

def count_surviving_support_factors(supports, rows, k):
    cols = ((1 << k)-1) ^ rows
    return sum(bool(left & ~rows) and bool(right & ~cols)
               for left, right in supports)

def build_factor_mode_groups(supports, k):
    selected = (1 << k)-1
    groups = []
    for left, right in supports:
        assert left and right
        modes = [(0, 0, 0)]
        if not left & ~selected:
            modes.append((left, 0, 1))
        if not right & ~selected:
            modes.append((0, right, 1))
        if len(modes) > 1:
            groups.append(modes)
    return groups

def complete_forced_cardinality_rows(positive, negative, ell, k):
    rows, needed = positive, ell-positive.bit_count()
    assert 0 <= needed <= k-(positive | negative).bit_count()
    for j in range(k):
        if needed and not (positive | negative) >> j & 1:
            rows |= 1 << j
            needed -= 1
    assert needed == 0 and rows.bit_count() == ell
    return rows

def decode_conditioning_branch_witness(groups, serial):
    positive = negative = killed = 0
    for group in reversed(groups):
        serial, digit = divmod(serial, len(group))
        p, n, d = group[digit]
        positive |= p
        negative |= n
        killed += d
    assert serial == 0 and not positive & negative
    return positive, negative, killed

def solve_conditioned_reservation_frontier(supports, k, frontier=True):
    r, selected = len(supports), (1 << k)-1
    groups = build_factor_mode_groups(supports, k)
    capacity = 1
    while capacity < k+1:
        capacity *= 2
    tags = [None]*(2*capacity) if frontier else None
    row_bill = 2*count_surviving_support_factors(supports, selected, k)+r+7*k+4
    col_bill = 2*count_surviving_support_factors(supports, 0, k)+r+7*k+4
    incumbent = min((row_bill, selected, 'row'), (col_bill, 0, 'col'))
    branches, consistent, largest_charge_slack = 0, 0, 0
    for serial, modes in enumerate(product(*groups)):
        branches += 1
        positive = negative = killed = 0
        for p, n, d in modes:
            positive |= p
            negative |= n
            killed += d
        if positive & negative:
            continue
        consistent += 1
        low, high, charge = positive.bit_count(), k-negative.bit_count(), r-killed
        ell = min(high, max(low, k//2))
        bill = 2*charge+2*r+3*k+4*max(ell, k-ell)+4
        if bill < incumbent[0]:
            rows = complete_forced_cardinality_rows(positive, negative, ell, k)
            incumbent = (bill, rows, 'mixed')
        # Demonstrates that a branch upper charge need not equal actual survival.
        witness = complete_forced_cardinality_rows(positive, negative, low, k)
        actual = count_surviving_support_factors(supports, witness, k)
        assert actual <= charge
        largest_charge_slack = max(largest_charge_slack, charge-actual)
        if frontier:
            left, right = capacity+low, capacity+high+1
            value = (charge, serial)
            while left < right:
                if left & 1:
                    tags[left] = value if tags[left] is None else min(tags[left], value)
                    left += 1
                if right & 1:
                    right -= 1
                    tags[right] = value if tags[right] is None else min(tags[right], value)
                left //= 2
                right //= 2
    bill, rows, kind = incumbent
    r0 = count_surviving_support_factors(supports, rows, k)
    actual_bill = (2*r0+r+7*k+4 if kind in ('row', 'col') else
                   2*r0+2*r+3*k+4*max(rows.bit_count(), k-rows.bit_count())+4)
    assert bill == actual_bill
    values = None
    if frontier:
        for node in range(1, capacity):
            for child in (2*node, 2*node+1):
                if tags[node] is not None:
                    tags[child] = (tags[node] if tags[child] is None else
                                   min(tags[child], tags[node]))
        values = [tags[capacity+i][0] for i in range(k+1)]
        for ell in range(k+1):
            cost, serial = tags[capacity+ell]
            p, n, killed = decode_conditioning_branch_witness(groups, serial)
            witness = complete_forced_cardinality_rows(p, n, ell, k)
            assert r-killed == cost
            assert count_surviving_support_factors(supports, witness, k) == cost
    return values, incumbent, (branches, consistent, largest_charge_slack, len(groups))

rng = Random(629)
fixtures = cardinalities = orientations = slack_cases = 0
max_branches = 0
for k in range(9):
    for case in range(36):
        r = 1+case % 5
        supports = [(rng.randrange(1, 1 << (k+1)), rng.randrange(1, 1 << (k+1)))
                    for _ in range(r)]
        values, incumbent, receipt = solve_conditioned_reservation_frontier(supports, k)
        oracle = [r+1]*(k+1)
        optimum = incumbent[0]
        for rows in range(1 << k):
            ell = rows.bit_count()
            r0 = count_surviving_support_factors(supports, rows, k)
            oracle[ell] = min(oracle[ell], r0)
            optimum = min(optimum, 2*r0+2*r+3*k+4*max(ell,k-ell)+4)
            orientations += 1
        assert values == oracle and incumbent[0] == optimum
        scalar_only = solve_conditioned_reservation_frontier(supports, k, False)
        assert scalar_only[1] == incumbent
        fixtures += 1
        cardinalities += k+1
        slack_cases += receipt[2] > 0
        max_branches = max(max_branches, receipt[0])

family = []
for k in (4, 6, 10, 1000):
    selected, universe = (1 << k)-1, (1 << (k+1))-1
    supports = [(universe, universe), (selected, selected)]
    _, winner, receipt = solve_conditioned_reservation_frontier(supports, k, False)
    assert winner[0] == 5*k+12 and winner[1].bit_count() == k//2
    assert receipt[0] == 3 and receipt[3] == 1
    assert winner[0] < 7*k+8  # Forced-R/forced-C only would miss this plan.
    family.append((k, receipt[0], winner[0], 7*k+8))

# An exterior membership is not available for masking by either selected side.
supports = [(3, 3)]
assert count_surviving_support_factors(supports, 0, 1) == 1
assert count_surviving_support_factors([(1, 1)], 0, 1) == 0
assert slack_cases > 0
print('factor_conditioning', dict(fixtures=fixtures, cardinality_checks=cardinalities,
      oracle_orientations=orientations, branch_slack_cases=slack_cases,
      maximum_branches=max_branches, mismatches=0))
print('family_k_branches_optimum_allrow', family)
print('mutations_no_free_branch_and_dropped_exterior=detected; k_zero=passed')
column_supports = [(3, 1), (5, 2)]
_, column_winner, _ = solve_conditioned_reservation_frontier(column_supports, 1)
assert column_winner == (15, 0, 'col')
print('reviewed_allcolumn_winner', column_winner, 'previous_portfolio=17')
```

## Verification Receipt

Use Python 3.10 or later (`int.bit_count` is required). The initial system-Python invocation stopped at that unavailable method before substantive assertions; it was an interpreter mismatch, not a passed test. The first Python 3.11 execution exited 0: 324 support fixtures, 1,620 cardinalities, 18,396 oracle orientations, 232 cases with a strictly conservative branch charge, maximum 72 visited branches, zero mismatches. The three-branch family produced `(k,slots,all-row-slots)` of `(4,32,36)`, `(6,42,50)`, `(10,62,78)` and `(1000,5012,7008)`; both deliberate mutations and k=0 passed. A subsequent execution after adding reconstruction of every full-frontier branch ID also exited 0 with those same counts and no mismatch. The envelope proof and finite probe have different scope from a numerically certified solve, runtime memory measurement, or global novelty assessment.

Completed independent review checked the two-sided envelope and clamped-cardinality proofs, and separately executed four support fixtures/all ten cardinalities, including empty factors list, incompatible forces and dual killing modes. It supplied the all-column candidate now admitted above. The lead then ran the revised retained probe: all existing counts remained green and the new explicit column witness returned `(15,0,'col')` versus the older portfolio's 17 slots. This is one scalar allowance where the old rejection interval was unnecessarily restrictive; it is not a physical RAM result.

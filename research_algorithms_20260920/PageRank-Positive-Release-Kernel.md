# Positive Evaluation Of Released PageRank Rows

Date: 2026-09-20. A01 algorithm refinement. This specifies an exact row-only release kernel that does not materialize the restored adjacency rows. It strengthens the storage side of [contraction-budgeted release](PageRank-Representation-Revision.md). The proof and finite oracle below are not a physical 4 GB benchmark or a publication-priority claim.

## The Problem Left By Explicit Release

Releasing a selected vertex j restores its original row `A[j,:]` as a destination-star factor. There are at most r heavy vertices when selection uses q_j>d_j, by the strengthened diagonal-dominance proof, but O(r) dense restored rows could still require O(Nr) edge records. Evaluating those rows as `U_j*(V^T y)-q_j*y_j` avoids stored edges but reintroduces a subtraction of large almost-equal values. That defeats the intended numerical improvement.

The revised kernel applies the SAME released factorization using original sparse factors, a selected-membership order and nonnegative leave-one-out sums. The ingredients are established sum reassociation and prefix/suffix evaluation; the candidate contribution is their use in a contraction-budgeted, factor-state PageRank execution plan. Full nearest-art comparison remains necessary.

## Static Format

After validating `A=UV^T-Q>=0`, retain:

1. Original entity-order U/V rows with d_i, q_i, p_i and original IDs, as in A01.
2. Sorted selected IDs R and their exact original self-loop weights `ell_j=(UV^T)[j,j]-q_j>=0`.
3. A factor-major selected stream. Each original factor f has selected destination memberships `(j,U_jf)` and selected source memberships `(i,V_if)`, each ordered by selected ID. Both lists contain only vertices in R.
4. The active base-coordinate map: keep f as an iterative coordinate only when `sum_(i outside R) U_if>0`. Original factors with zero mass outside R may still be needed to evaluate restored rows; do not erase their selected memberships.

Let Z be original U/V nonzeros, Z_UR and Z_VR the selected subsets, and k=|R|. Extra membership records number `Z_UR+Z_VR<=Z`, not Nk. IDs, offsets, scalar widths and metadata still add bytes. Building the selected order needs a bounded join/filter and external sort; scratch and old/new coexistence are charged. It is not a zero-cost second index, nor automatically within the 50 GB retained portfolio.

One selected factor group has at most k distinct source IDs and k destination IDs. A group can therefore use O(k) scratch, independent of N and the full factor's degree. Duplicate membership entries must first be combined under the source contract. A factor-ID directory need not be resident if groups are processed sequentially.

Compile group IDs to dense selected ordinals 0..k-1 for constant-cost y/next access. During the entity-order pass, a cursor over the sorted selected-ID stream identifies selected rows without a per-row binary search or unpriced hash-table guarantee. Original-ID conversion and construction of these maps are build work, not hidden in the linear query-work bound.

## The Positive Two-Pass Kernel

Let h contain active base coordinates, one coordinate h_R[j] per selected row, and dangling mass h_d. Initialize using the gather below applied to x=p, so the warm start remains the released `Vbar^T p`.

During the ordinary row pass, reconstruct and immediately consume one score:

```text
if i in R:
    x_i = (1-a)*p_i + a*(h_R[i] + p_i*h_d)
else:
    x_i = H_i*((1-a)*p_i + a*(U_i*h_base + p_i*h_d))

y_i = x_i/d_i if d_i>0, else 0
```

Gather `g_off[f]=sum_(i outside R) V_if*y_i`, retain y_j only for j in R, and accumulate dangling x_i over all zero-degree sources. There is no stored N-score vector. Initialize each new row coordinate to `ell_j*y_j`.

In the selected factor-major pass, for each original factor f:

```text
values = [(i, V_if*y_i) for selected sources in this group]
build prefix and suffix sums of values
selected_total = sum(values)

for each selected destination (j,U_jf):
    if j occurs in values:
        other = prefix_before_j + suffix_after_j
    else:
        other = selected_total
    next_R[j] += U_jf*(g_off[f] + other)

if f has an active base coordinate:
    next_base[f] = g_off[f] + selected_total
```

Sorted source/destination IDs permit a merge cursor; there is no per-group N-sized lookup. Include factors with no selected sources or destinations: their remaining branch still contributes normally. A released row with no actual incoming edges can retain a harmless zero-gather coordinate, or be removed with an explicit map; its selected-ID descriptor must remain for interpreting the release.

The current h and next are distinct. g_off needs r original-factor slots even when some factors have no active base coordinate. Selected y needs k slots; prefix/suffix scratch needs at most two k-sized scalar arrays plus bounded IDs/weights. No independent per-thread replicas are permitted without a revised reservation.

## Why This Applies The Exact Released Operator

The released row's gather is `sum_i A[j,i]*y_i`. Since Q is diagonal,

```text
sum_i A[j,i]*y_i
  = ell_j*y_j + sum_f U_jf * sum_(i != j) V_if*y_i
  = ell_j*y_j + sum_f U_jf * (
        g_off[f] + sum_(i in R, i != j) V_if*y_i).
```

This is exactly the second pass. For a retained base factor, its V column is unchanged by row-only release, so `g_off+selected_total` is exactly its gather. The reconstruction formula follows because selected original rows are represented by unit-vector U columns, and original U rows are zero there. Dangling redistribution is unchanged.

For the base fixed-point schedule, the warm start, p and all reconstructed/gathered values are nonnegative. Every kernel multiplication and summation above is nonnegative, including the admitted original loop weight ell. It avoids dynamic subtraction of `q_j*y_j` and avoids deriving an exclusive sum as total-minus-own. Arbitrary signed h still satisfies the algebra, but does not have this positivity property; that is relevant to other solvers and invalid warm starts.

Positivity does not prove a floating certificate. Accumulation, conversion, overflow/underflow, the source-degree calculation, and output rounding still need enclosures or declared error bounds. Computing ell_j and d_i must use checked source-exact arithmetic or validated enclosures. A positive runtime kernel cannot undo an incorrectly published coefficient. The exact PageRank error and contraction theorems apply to the intended operator; numerical implementation remains a separate gate.

## Resource And Work Bound

Let r0 be the number of active original base coordinates, r0<=r. A conservative scalar-payload reservation for the kernel is:

```text
h and next:          2*(r0+k+1)
off-selected gather: r
selected y:          k
prefix/suffix:       2*(k+1)
group input weights: at most 2*k
total:               2*r0 + r + 7*k + 4 scalars
```

For heavy release k<=r, this is at most `10r+4` scalar slots. It is not the whole worker's bytes: selected IDs, active-coordinate maps, group input IDs/headers, runtime, certificate weights, compensation/enclosure widths, output and allocator headroom are additional explicit charges. At r=1M, this bound is 80,000,032 bytes of f64 scalar payload, not an 80 MB application guarantee. Enclosures or compensation can materially increase it. This conservative count allows both selected lists' weights to be decoded simultaneously; streaming the destination list can reduce that allowance.

One base update has logical arithmetic `O(N+Z+Z_UR+Z_VR+r+k)`. It reads the ordinary row stream and one selected-factor stream, with bounded per-group passes through resident selected scratch. The selected stream is no larger in membership count than the original factors. There is no restored-edge expansion and no full original row replay per selected vertex. File offsets, page effects, variable-width arithmetic and temporary builder work remain outside this logical count and must be measured.

The cheap residual weights are `c_base[f]=sum_(i outside R) U_if`, `c_R[j]=1`, and `c_d=1`. The tighter original residual scan uses old U rows outside R, unit-vector selected rows, and p; it does not need the implicit dense V row factors. It still requires an additional row pass and the correct h/delta lifetime.

Thus this row-only kernel combines three conditional guarantees: at most 2r abstract factors for heavy release, beta<=2a/(1+a), and no O(Nr) restored-edge artifact. It does not guarantee cheap initial factor discovery, a given output deadline, or a win over a more competent low-rank solver. The follow-up independent review validates this stronger bound and the positive kernel while retaining the numerical and physical implementation gates.

## Exact Executable Check

The code uses helper definitions in the sole Python block of `PageRank-Representation-Revision.md`. Extract that block followed by this block into one Python process. The first block prints its own receipt; the second prints this kernel's receipt. Both use only Python's standard library. The fixture arrays are oracle inputs, not a measurement of a physical storage system.

```python
def prepare_sparse_release_fixture(U, V, q, p, selected):
    n,r = len(p),len(U[0])
    oracle = build_exact_pagerank_operator(U,V,q,p,F(1,2))
    rows = [(i,oracle['d'][i],q[i],p[i],
             [(f,U[i][f]) for f in range(r) if U[i][f]],
             [(f,V[i][f]) for f in range(r) if V[i][f]]) for i in range(n)]
    groups = [(f,[(i,V[i][f]) for i in sorted(selected) if V[i][f]],
                 [(i,U[i][f]) for i in sorted(selected) if U[i][f]]) for f in range(r)]
    active = [f for f in range(r) if any(U[i][f] for i in range(n) if i not in selected)]
    loops = {i:oracle['A'][i][i] for i in selected}
    return rows,groups,loops,active,r


def apply_positive_released_kernel(plan, a, selected, h=None):
    rows,groups,loops,active,r = plan
    ids = sorted(selected)
    where = {v:j for j,v in enumerate(ids)}
    amap = {f:j for j,f in enumerate(active)}
    k, r0 = len(ids), len(active)
    goff = [F(0)]*r
    yr = [F(0)]*k
    next_row = [F(0)]*k
    dangling = F(0)
    source_visits = destination_visits = selected_visits = 0
    for i,di,qi,pi,urow,vrow in rows:
        if h is None:
            xi = pi
        else:
            value = h[r0+where[i]] if i in selected else sum(weight*h[amap[f]] for f,weight in urow)
            Hi = F(1) if i in selected or not di else 1/(1+a*qi/di)
            xi = Hi*((1-a)*pi+a*(value+pi*h[-1]))
        yi = xi/di if di else F(0)
        if not di:
            dangling += xi
        if i in selected:
            yr[where[i]] = yi
            next_row[where[i]] = loops[i]*yi
        else:
            for f,weight in vrow:
                goff[f] += weight*yi
                source_visits += 1
    next_base = [F(0)]*r0
    max_group = 0
    for f,sources,destinations in groups:
        selected_visits += len(sources)
        destination_visits += len(destinations)
        max_group = max(max_group,len(sources))
        prefix = [F(0)]
        for i,v in sources:
            prefix.append(prefix[-1]+v*yr[where[i]])
        suffix = [F(0)]*(len(sources)+1)
        for j in range(len(sources)-1,-1,-1):
            suffix[j] = suffix[j+1]+sources[j][1]*yr[where[sources[j][0]]]
        cursor = 0
        for j,weight in destinations:
            while cursor < len(sources) and sources[cursor][0] < j:
                cursor += 1
            other = prefix[cursor]+suffix[cursor+1] if cursor < len(sources) and sources[cursor][0] == j else prefix[-1]
            next_row[where[j]] += weight*(goff[f]+other)
        if f in amap:
            next_base[amap[f]] = goff[f]+prefix[-1]
    assert max_group <= k
    return next_base+next_row+[dangling], active, dict(group=max_group,
        source_visits=source_visits, selected_source_visits=selected_visits,
        selected_destination_visits=destination_visits)


kernel_out = dict(seed=20260921, fixtures=0, warm_checks=0, signed_checks=0,
                  positive_checks=0, positive_trajectory_checks=0, max_group=0)
krng = Random(kernel_out['seed'])
for trial in range(128):
    n,r = krng.randrange(1,8),krng.randrange(5)
    U = [[F(krng.randrange(4)) for f in range(r)] for i in range(n)]
    V = [[F(krng.randrange(4)) for f in range(r)] for i in range(n)]
    q = [sum(u*v for u,v in zip(U[i],V[i]))*krng.choice([F(0),F(1,3),F(1)]) for i in range(n)]
    p = [F(krng.randrange(4)) for i in range(n)]
    p[0] += 1
    total = sum(p)
    p = [x/total for x in p]
    a = krng.choice([F(1,10),F(17,20),F(97,100)])
    selected = {i for i in range(n) if krng.randrange(2)}
    plan = prepare_sparse_release_fixture(U,V,q,p,selected)
    warm,active,counters = apply_positive_released_kernel(plan,a,selected)
    old = build_exact_pagerank_operator(U,V,q,p,a)
    ids = sorted(selected)
    Ur = [[F(0) if i in selected else U[i][f] for f in active]+[F(i==j) for j in ids] for i in range(n)]
    Vr = [[V[i][f] for f in active]+[old['A'][j][i] for j in ids] for i in range(n)]
    qr = [F(0) if i in selected else q[i] for i in range(n)]
    op = build_exact_pagerank_operator(Ur,Vr,qr,p,a)
    assert op['A'] == old['A']
    assert warm == [sum(v*pi for v,pi in zip(row,p)) for row in op['Vt']]
    kernel_out['warm_checks'] += 1
    for signed in [False,True]:
        h = [F(krng.randrange(-4 if signed else 0,5),krng.randrange(1,5)) for _ in warm]
        value,_,counters = apply_positive_released_kernel(plan,a,selected,h)
        expected = [op['g'][j]+sum(m*v for m,v in zip(row,h)) for j,row in enumerate(op['M'])]
        assert value == expected
        if signed:
            kernel_out['signed_checks'] += 1
        else:
            assert all(x>=0 for x in value)
            kernel_out['positive_checks'] += 1
    h = warm
    for iteration in range(5):
        value,_,counters = apply_positive_released_kernel(plan,a,selected,h)
        assert value == [op['g'][j]+sum(m*v for m,v in zip(row,h)) for j,row in enumerate(op['M'])]
        assert all(v>=0 for v in value)
        h = value
        kernel_out['positive_trajectory_checks'] += 1
    kernel_out['fixtures'] += 1
    assert counters['source_visits']+counters['selected_source_visits'] == sum(bool(x) for row in V for x in row)
    assert counters['selected_destination_visits'] == sum(bool(U[i][f]) for i in selected for f in range(r))
    kernel_out['max_group'] = max(kernel_out['max_group'],counters['group'])

K = F(2**54)
U,V,q = [[K],[F(1)]],[[F(1)],[1/K]],[K,1/K]
p,a,R = [F(1),F(0)],F(17,20),{0}
plan = prepare_sparse_release_fixture(U,V,q,p,R)
warm,_,_ = apply_positive_released_kernel(plan,a,R)
step,_,_ = apply_positive_released_kernel(plan,a,R,warm)
assert all(x>=0 for x in step)
kernel_out['huge_diagonal_exact_check'] = True
print(json.dumps(kernel_out,indent=2))
```

## Verification Status

The saved two-block extraction executed successfully with exit status 0. The second receipt was:

```json
{
  "seed": 20260921,
  "fixtures": 128,
  "warm_checks": 128,
  "signed_checks": 128,
  "positive_checks": 128,
  "positive_trajectory_checks": 640,
  "max_group": 6,
  "huge_diagonal_exact_check": true
}
```

All comparisons were exact rationals with zero mismatches. The sparse kernel consumes prebuilt sparse row/group inputs; dense source and explicit released matrices are constructed outside it only for the oracle. Warm starts, arbitrary signed states, nonnegative states, five-step positive trajectories, selected dangling vertices and vanished base-coordinate cases are covered by the generated fixtures. The final separate fixture uses K=2^54 and exact admitted coefficients. That last result is not a float implementation claim.

The kernel also checks the group-size bound and source/destination membership-visit identities. Its in-memory fixture adapter does not establish the external sort, actual bounded decoder, file offset correctness or full physical ownership. Directed rounding, static format construction, concurrent memory ownership, encoded page counts and a physical machine limit remain separate implementation obligations.

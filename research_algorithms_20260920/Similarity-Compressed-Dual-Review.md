# Similarity Compressed-Dual Independent Review

Date: 2026-09-21. Independent theorem composition review. This is the sole file
owned in this new sidecar. The preceding workload profile is frozen. No lead
compressed-dual/modal implementation or tests are read, imported, or executed.

## Verdict

Accept the composition, conditional on a trusted, correctly prepared additive
core and the exact two-row attained-maxima contract. A bundle is a generalized
reward leaf, not a physical leaf with query count B. Its two marginal threshold
tables preserve precisely the information the inverse DP needs, including a
valid companion for each separately scored row.

The uniform post-preparation bound is

```text
arithmetic operations: O((r+1)(Q+1) + Q^2)
temporary cells:       O((r+1)(Q+1))
static records:        O(r+d+1)
```

The requested `O(r+Q(r+1)+Q^2)` needs an additive constant at r=Q=0. This is
otherwise the same bound. Q is numerical query/union cardinality, not its bit
length. No physical memory, runtime, implementation, or publication-priority
claim follows.

## Contract And Dependencies

Read mathematical inputs: [Additive-Region Core](Similarity-Additive-Region-Core.md),
[Additive-Core Review](Similarity-Additive-Core-Review.md),
[Overlap Frontier Compression](Similarity-Overlap-Frontier-Compression.md),
[Overlap-Frontier Review, Section 9](Similarity-Overlap-Frontier-Review.md), and
[Novelty Evidence Policy](Novelty-Baseline-Evidence-Policy.md). These provide
reviewed contraction and inverse-threshold results, not independent prior-art
evidence for this new composition. The modal preparation contract is the one
independently examined in the now-frozen workload sidecar.

There are exactly two rows, a fixed partitioned union, and an attained maximum
C_v at every original node. Root sizes are u=C_root and ell<=u; source size a
includes outside-union features. Query counts q_j are valid original leaf
counts, Q=sum q_j<=a. The complete envelope maximizes the score of either row
over all compatible two-row realizations. Infeasible metadata has no envelope;
it is not assigned the numeric score zero. Zero overlap scores zero, including
empty/empty, under the existing contract.

The prepared core retains every strict node, every contracted additive component,
and a bundle (L,U,A,B) on each component. With b=L-A, the bundle statistics are

```text
L = sum ordinary-leaf (H_j-C_j)      U = sum ordinary-leaf C_j
b = sum min(q_j,H_j-C_j)             B = sum min(q_j,C_j)
0 <= b <= L <= U,   b <= B <= U,    B-b <= U-L.
```

The original hard L,U are not clipped. Modal signatures recover the two reward
sums for admitted q<=tau without retaining a G-entry query vector. Trusted
preparation has already validated leaf IDs, ordering, uniqueness, q<=H, total
Q<=a, ownership, original topology and original numerical metadata. Those costs
are not silently included in the post-preparation DP bound.

## Why The Bundle Suffices

For the ordinary leaves of one component, a common major row has count U, and
the other row can have every integer count x in [L,U]. Their exact union size
is H*=L+U, so their intersection is e=x-L and K*=U-L. Count feasibility is
therefore exactly e=0,...,K*. This includes the empty identity bundle.

At fixed e, the major row's best overlap is B; the minor row's best overlap is

```text
b + min(e,B-b) = min(b+e,B).
```

The additive-core lifting argument proves these are attained. Allocate minor
increments to gain-one leaf prefixes before gain-zero suffixes. For a chosen
scored row, realize its selected features query-first. At each original leaf,
complete the companion with all missing union elements and the required number
of shared elements. The companion has its declared count, every leaf union is
covered, and every ordinary major maximum remains attained. Major and minor
optima may use different allocations and different pairs; that is sufficient.

Thus the exact generalized-leaf thresholds, for 0<=c<=B, are

```text
T_major[c] = 0
T_minor[c] = max(0,c-b).
```

All c>B are impossible. The inequality B-b<=U-L keeps every finite threshold
inside the feasible intersection domain. **Do not use min(q,x) for an invented
aggregate q.** For (L,U,A,B)=(2,4,1,2), the minor values at x=2,3,4 are 1,2,2;
a physical aggregate H=6,C=4,q=3 gives the wrong values 2,3,3.

Each role's feasible at-least-c intersections form a tail interval ending at K*.
Consequently a minimum threshold witness extends to any larger required e
without losing the requested reward. This is proved directly for the bundle;
it does not require pretending its reward function is a physical-leaf reward.

## Composition And Companion Validity

Derive H for a component by summing L+U and its strict-boundary subtree H values;
its C equals U plus the boundary C values. At strict nodes H is the sum of child
H and C remains the original quota. These identities are available from the
core in O(r+1) arithmetic operations. The root H is still the original union
population, not a surrogate that changes I=ell+u-H.

Induction invariant: for each retained subtree, role R and intersection e, the
table's recovered value is the maximum scored-row overlap among **complete
compatible pairs** with that row in role R. If no such pair exists, it is
infeasible. Each finite value has an actual original-leaf witness and a valid
companion; different entries need not share a witness.

At an additive component, all boundary subtrees and the bundle have aligned
roles. Their features are disjoint, so separately chosen witness pairs can be
united with their scored rows aligned. Their companion counts add as well.
The component major count equals the sum of the caps; every removed interior
quota follows from the reviewed boundary-tuple projection. Conversely every
original pair projects to such a tuple. Binary min-plus merging of the two
role tables therefore preserves the invariant, including nonzero feasibility
thresholds inherited from strict boundaries.

Explicitly, `T_R[c]=min_{i+j=c}(T_R,left[i]+T_R,right[j])`. At-least-c
semantics need no larger index sums: any witness with overlaps totaling at
least c permits smaller nonnegative requirements totaling exactly c. Conversely
the two threshold witnesses achieve at least their summed requirements.

At a strict node use the reviewed exact recurrence. To make its domain guards
explicit, write child caps C_l,C_r, D=C_l+C_r-C>0, K_i=2C_i-H_i, and

```text
alpha = K_l-D,   beta = K_r-D,   K_parent = alpha+beta.
V_R,i(t) = largest c with T_R,i[c]<=t, or infeasible.
```

For each c, take the minimum valid branch below:

```text
major: beta  + T_M,l[max(0,c-V_m,r(beta))],  require residual <= alpha
       alpha + T_M,r[max(0,c-V_m,l(alpha))], require residual <= beta
minor: alpha + T_m,r[max(0,c-V_M,l(alpha))], require residual <= beta
       beta  + T_m,l[max(0,c-V_M,r(beta))],  require residual <= alpha.
```

Here "residual" means the retrieved threshold, not the residual score index.
Reject negative alpha/beta, infeasible fixed-child values, out-of-range indexes,
infinite residual thresholds, and cap violations. The fixed child's best
scored-row witness can be paired with the variable child's threshold witness.
The cap guard and parent equality impose the required companion maximum as
well as the scored-row count. No step asks for both rows' optimal rewards.

For any larger parent intersection up to K_parent, increase the variable child's
intersection while retaining its requested reward; the fixed child stays fixed.
Thus a finite branch is a tail interval. Taking the union of the branches is
again a tail interval. Additive composition has the same property because sums
of integer tail intervals are tail intervals. This proves exact inverse recovery
throughout the generalized core without assuming concavity or unit jumps.

At the root require I in [0,K_root] and both recovered roles feasible. The two
zero thresholds agree and may be positive. Recover t_M,t_m at I and return

```text
max(t_M/(a+u-t_M), t_m/(a+ell-t_m)), with zero-overlap score zero.
```

Necessity follows by projecting any original compatible pair. Sufficiency
follows by lifting the chosen role's complete witness inductively. Hence both
feasibility and the complete maximum-of-either-row envelope are exact. Joint
two-row rewards, a common optimizer for both roles, and expanded witness output
are deliberately outside this theorem.

## Scheduling And Size

Let P=sum over component bundles B. Then P<=Q, and each subtree can index only
0,...,P_v, where P_v sums descendant bundle B values. This is available without
the discarded original leaf q table. States c>P_v are implicitly impossible.
Using P instead of Q can make the actual schedule smaller.

The structural core has at most 3r+1 nodes and 2r+1 components. View every bundle
as one virtual leaf and fold each component's strict children into its bundle
accumulator one at a time. This creates O(r+1) virtual nodes/merges, not a
Cartesian product over all children. Direct strict-to-strict edges stay direct.
No original-G walk or original-height factor is needed after preparation.

More precisely, if K is the structural node count and E_component is the number
of component-to-strict edges, the expanded plan has
`V_plan=K+E_component<=2K-1` nodes: one bundle node per component, one node per
strict vertex, and one additive merge per component child. Retaining all plan
tables would allocate exactly `2*sum_v(P_v+1)` threshold cells. Here P denotes
score-support mass, not the lead's use of P for plan-node count.

Each strict node takes O(P_v+1) work: scan fixed-child tables once, then perform
constant-time residual lookups per c. Each binary additive fold with supports
p,t takes O((p+1)(t+1)) work. On the resulting binary expression tree,

```text
sum over all binary nodes p*t = (P^2-sum bundle B^2)/2 <= P^2/2.
```

Summing only additive nodes cannot increase that charge. The linear support
terms are O(P(r+1)), even if the contracted/virtual tree is a chain. Initializing
zero states and visiting structural records cost O(r+1). Thus
O((r+1)(P+1)+P^2), and hence the stated Q bound, follows. Arbitrarily high-degree
components do not introduce another factor r if merged by this schedule.

Retain two arrays per structural subtree and a current component accumulator;
discard overwritten fold scratch. All table lengths are <=P+1 and there are
O(r+1) retained arrays/records, giving O((r+1)(P+1)) temporary cells. This is a
safe upper bound, not an optimal lifetime or byte claim. Allocate only after a
paid support/work preflight; refusal is not an envelope and never licenses
pruning. Static core, owner runs, modal defaults and d deviations remain
O(r+d+1) records. Preparation/input ownership is separate from evaluator state.

## Limits And One Alternative

For a large-r/small-Q separation, start with an exclusive row-0 leaf of size two.
Repeatedly define T_k=(p_k,S_k), S_k=(n_k,T_(k-1)), where p_k is an exclusive
row-0 singleton and n_k an exclusive row-1 singleton. T_k is additive and S_k
has strict deficit one. There are r=k strict nodes, G=2r+1 leaves, u=r+2,
ell=r, and I=0. Query one feature of the base leaf: Q=a=1, and the exact
envelope is 1/(r+2). The larger-capacity child is forced to be major along the
ladder, so the base query lies in the root major row. The threshold schedule is
linear in r; the previous unconditional assignment schedule reserves 2^r cases.
This separates those schedules, not all possible feasibility-aware algorithms.
Additive zero-leaf refinements can increase G without increasing r or P, with
the original build still paid.

Conversely, a single leaf with H=C=N, ell=N and q=a=N has r=0 and answer one.
The generic dual allocates 2(N+1) threshold cells, whereas the scalar bundle
formula or one-assignment core path is constant-size. For N=10^12 the dense
dual must be rejected before allocation; no such table is executed here.
Small r and huge binary-encoded Q are a negative regime, not a corner to hide.

The one alternative considered is to retain the reviewed orientation-core
evaluator and select by conservative, paid reservations between its
O((r+1)2^r) arithmetic bound and the compressed dual bound. This selection does
not change either envelope, eliminate refusal, or prove a speed improvement.

For w=ceil(log2(2+a+H_root+G)), scalar arithmetic/comparison has O(w) bit cost.
The displayed DP schedule has bit cost
O(((r+1)(P+1)+P^2)*w + M(w)), with M(w) for the final rational cross-products;
optional fraction reduction adds its gcd cost. Threshold payload is
O((r+1)(P+1)*w) bits, excluding object overhead. Static modal build can use
deterministic sorting in O(G log G) comparisons and O(G) workspace, plus the
paid original validation and raw feature/count work. This is not a polynomial
kernel in count bit length or a sublinear raw-input claim.
The supplied profile parameter tau and other build inputs have their own bit
widths; the displayed w is for the post-preparation DP, not a bound on every
possible upstream configuration value.

The prepared-core boundary is material: a forged graph, duplicated child,
misowned leaf, clipped hard L/U, invalid bundle, or stale query reward record
does not become valid because local scalar inequalities pass. The theorem
assumes the reviewed compiler/preparer contract. The DP still detects global
count infeasibility within that contract, including with Q=0. Explicit witness
expansion needs original leaf information and its output-size cost.
The small-state contract also excludes retained original-tree objects, G-sized
node keys, input-owning closures and failure tracebacks from the returned core.
Use compact core IDs; successful mathematical contraction alone is not an
object-lifetime audit.

Profit-indexed inverse DP, min-plus convolution, saturation, and additive
contraction remain credited to their established/reviewed foundations. The
claim examined here is their specific exact composition and parameter bound.
No new generic primitive, external-code comparison, or priority claim is made.

## Retained Independent Checker

The following snippet reads no project files and imports only the Python
standard library. Its compiler is an independent mathematical construction,
not the lead compiler.
It compares every retained-subtree threshold against a direct original-tree
(intersection, scored-row count) DP, and root frontiers against complete actual
feature-set pairs grouped by their attained maxima. It also checks root scores
at every possible intersection with zero or two outside-union query features.
This checker deliberately retains original arrays and tuple-shaped original
node keys to align subtree oracle comparisons. It is not a compressed storage
implementation or an ownership measurement. Those diagnostic keys are not part
of the claimed production core representation. Named adversarial metadata and
a symbolic-size negative case are included; no public timing or large numeric
array is produced.
There are six fixed layouts, with leaf populations (0,0,0,0), (3), (1,3),
(1,2,1,2), (1,0,1,1,0,1,0,1), and (3,3), and the explicit trees in the code.
For each layout every ordered pair covering that exact union is enumerated.
Queries use every feasible leaf count vector and a canonical feature subset
within each leaf; full pair enumeration makes within-leaf query permutations
equivalent. The dense original-tree control incorporates no contraction or
inverse recurrence. The complete feature-set root oracle supplies a second,
independent check beyond that constructed DP control.

```python
from collections import Counter, defaultdict
from fractions import Fraction
from itertools import product

checks = Counter()
coverage = set()


def list_original_nodes_postorder(tree):
    if isinstance(tree, int):
        return [tree]
    return list_original_nodes_postorder(tree[0]) + list_original_nodes_postorder(tree[1]) + [tree]


def accumulate_original_leaf_values(tree, values):
    output = {}
    for node in list_original_nodes_postorder(tree):
        output[node] = values[node] if isinstance(node, int) else sum(output[c] for c in node)
    return output


def build_original_dense_frontiers(tree, populations, capacities, queries):
    result = {}
    for node in list_original_nodes_postorder(tree):
        h, cap = populations[node], capacities[node]
        k = 2 * cap - h
        states = {}
        if isinstance(node, int):
            for e in range(k + 1):
                for size in {cap, h + e - cap}:
                    states[e, size] = min(queries[node], size)
        else:
            for (el, xl), vl in result[node[0]].items():
                for (er, xr), vr in result[node[1]].items():
                    e, size = el + er, xl + xr
                    if e <= k and max(size, h + e - size) == cap:
                        states[e, size] = max(states.get((e, size), -1), vl + vr)
        result[node] = states
    return result


def compile_independent_additive_core(tree, populations, capacities, queries):
    strict = {v for v in list_original_nodes_postorder(tree)
              if not isinstance(v, int) and sum(capacities[c] for c in v) > capacities[v]}
    if not strict:
        coverage.add("r_zero")
    for v in strict:
        if any(c in strict for c in v):
            coverage.add("adjacent_strict")

    def contract_original_subtree_nodes(v):
        if v in strict:
            return {"node": v, "strict": True, "C": capacities[v],
                    "children": [contract_original_subtree_nodes(c) for c in v]}
        leaves, boundaries = [], []

        def collect_component_boundary_nodes(w):
            if w in strict:
                boundaries.append(contract_original_subtree_nodes(w))
            elif isinstance(w, int):
                leaves.append(w)
            else:
                for child in w:
                    collect_component_boundary_nodes(child)

        collect_component_boundary_nodes(v)
        lower = sum(populations[j] - capacities[j] for j in leaves)
        upper = sum(capacities[j] for j in leaves)
        base = sum(min(queries[j], populations[j] - capacities[j]) for j in leaves)
        top = sum(min(queries[j], capacities[j]) for j in leaves)
        if lower > base:
            coverage.add("positive_A")
        if not leaves:
            coverage.add("empty_bundle")
        if len(boundaries) > 1:
            coverage.add("multiple_boundaries")
        return {"node": v, "strict": False, "C": capacities[v],
                "bundle": (lower, upper, lower - base, top), "children": boundaries}

    return contract_original_subtree_nodes(tree), len(strict)


def create_threshold_table_record(h, cap, major, minor, stats):
    assert len(major) == len(minor)
    assert major[0] == minor[0]
    stats["tables"] += 1
    stats["initialized_cells"] += len(major) + len(minor)
    return {"H": h, "C": cap, "P": len(major) - 1, "M": major, "m": minor}


def compute_threshold_inverse_value(record, role, intersection):
    if not 0 <= intersection <= 2 * record["C"] - record["H"]:
        return None
    values = [c for c, threshold in enumerate(record[role])
              if threshold is not None and threshold <= intersection]
    return max(values, default=None)


def merge_additive_threshold_records(left, right, stats):
    tables = {}
    for role in ("M", "m"):
        table = [None] * (left["P"] + right["P"] + 1)
        for i, first in enumerate(left[role]):
            for j, second in enumerate(right[role]):
                stats["convolution_pairs"] += 1
                if first is not None and second is not None:
                    value = first + second
                    if table[i + j] is None or value < table[i + j]:
                        table[i + j] = value
        tables[role] = table
    return create_threshold_table_record(left["H"] + right["H"], left["C"] + right["C"],
                                         tables["M"], tables["m"], stats)


def merge_strict_threshold_records(left, right, cap, stats):
    deficit = left["C"] + right["C"] - cap
    assert deficit > 0
    alpha = 2 * left["C"] - left["H"] - deficit
    beta = 2 * right["C"] - right["H"] - deficit
    size = left["P"] + right["P"] + 1
    tables = {role: [None] * size for role in ("M", "m")}
    if alpha >= 0 and beta >= 0:
        branches = {
            "M": [(left, "M", right, "m", beta, alpha),
                  (right, "M", left, "m", alpha, beta)],
            "m": [(right, "m", left, "M", alpha, beta),
                  (left, "m", right, "M", beta, alpha)]}
        for role, choices in branches.items():
            for variable, vr, fixed, fr, offset, limit in choices:
                reward = compute_threshold_inverse_value(fixed, fr, offset)
                if reward is None:
                    continue
                for c in range(size):
                    stats["strict_branches"] += 1
                    need = max(0, c - reward)
                    threshold = variable[vr][need] if need <= variable["P"] else None
                    if threshold is not None and threshold <= limit:
                        value = offset + threshold
                        if tables[role][c] is None or value < tables[role][c]:
                            tables[role][c] = value
    return create_threshold_table_record(left["H"] + right["H"], cap,
                                         tables["M"], tables["m"], stats)


def evaluate_independent_core_thresholds(core, stats, retained):
    children = [evaluate_independent_core_thresholds(c, stats, retained) for c in core["children"]]
    if core["strict"]:
        result = merge_strict_threshold_records(children[0], children[1], core["C"], stats)
    else:
        lower, upper, forced, top = core["bundle"]
        base = lower - forced
        assert 0 <= base <= lower <= upper and base <= top <= upper
        assert top - base <= upper - lower
        result = create_threshold_table_record(lower + upper, upper, [0] * (top + 1),
                                               [max(0, c - base) for c in range(top + 1)], stats)
        for child in children:
            result = merge_additive_threshold_records(result, child, stats)
        assert result["C"] == core["C"]
    retained[core["node"]] = result
    return result


def compute_exact_fraction_score(overlap, row_size, source_size):
    return Fraction(overlap, source_size + row_size - overlap) if overlap else Fraction(0)


def recover_exact_root_envelope(record, intersection, source_size):
    major = compute_threshold_inverse_value(record, "M", intersection)
    minor = compute_threshold_inverse_value(record, "m", intersection)
    assert (major is None) == (minor is None)
    if major is None:
        return None
    return max(compute_exact_fraction_score(major, record["C"], source_size),
               compute_exact_fraction_score(minor, record["H"] + intersection - record["C"], source_size))


def check_complete_feature_family(tree, leaf_populations):
    nodes = list_original_nodes_postorder(tree)
    h = accumulate_original_leaf_values(tree, leaf_populations)
    leaf_features = []
    next_feature = 0
    for count in leaf_populations:
        leaf_features.append(tuple(range(next_feature, next_feature + count)))
        next_feature += count
    masks = accumulate_original_leaf_values(tree, [sum(1 << f for f in fs) for fs in leaf_features])
    summaries = defaultdict(list)
    for memberships in product((1, 2, 3), repeat=next_feature):
        first = sum(1 << f for f, label in enumerate(memberships) if label & 1)
        second = sum(1 << f for f, label in enumerate(memberships) if label & 2)
        caps = tuple(max((first & masks[v]).bit_count(), (second & masks[v]).bit_count()) for v in nodes)
        summaries[caps].append((first, second))
        checks["ordered_full_union_pairs"] += 1
    checks["metadata_summaries"] += len(summaries)
    for caps, pairs in summaries.items():
        cap = dict(zip(nodes, caps))
        for query in product(*(range(n + 1) for n in leaf_populations)):
            checks["summary_query_cases"] += 1
            assert checks["summary_query_cases"] <= 15000
            query_totals = accumulate_original_leaf_values(tree, query)
            query_mask = sum(1 << f for fs, count in zip(leaf_features, query) for f in fs[:count])
            if not sum(query):
                coverage.add("zero_query")
            dense = build_original_dense_frontiers(tree, h, cap, query)
            core, r = compile_independent_additive_core(tree, h, cap, query)
            stats, retained = Counter(), {}
            root = evaluate_independent_core_thresholds(core, stats, retained)
            assert len(retained) <= 3 * r + 1
            for node, record in retained.items():
                assert record["H"] == h[node]
                for role in ("M", "m"):
                    for c in range(query_totals[node] + 1):
                        wanted = [e for (e, size), value in dense[node].items()
                                  if size == (cap[node] if role == "M" else h[node] + e - cap[node])
                                  and value >= c]
                        actual = record[role][c] if c <= record["P"] else None
                        assert actual == min(wanted, default=None)
                        checks["subtree_threshold_entries"] += 1
                    for e in range(2 * cap[node] - h[node] + 1):
                        size = cap[node] if role == "M" else h[node] + e - cap[node]
                        assert compute_threshold_inverse_value(record, role, e) == dense[node].get((e, size))
                        checks["subtree_inverse_values"] += 1
            actual_frontier = {}
            for first, second in pairs:
                state = ((first & second).bit_count(), first.bit_count())
                actual_frontier[state] = max(actual_frontier.get(state, -1), (first & query_mask).bit_count())
            assert actual_frontier == dense[tree]
            checks["actual_root_frontiers"] += 1
            for e in range(2 * cap[tree] - h[tree] + 1):
                if e == 2 * cap[tree] - h[tree]:
                    coverage.add("root_tie")
                for outside in (0, 2):
                    a = sum(query) + outside
                    values = [compute_exact_fraction_score(value, size, a)
                              for (shared, size), value in actual_frontier.items() if shared == e]
                    expected = max(values, default=None)
                    assert recover_exact_root_envelope(root, e, a) == expected
                    checks["root_envelopes"] += 1
                    if expected is None:
                        checks["infeasible_root_envelopes"] += 1


families = [(((0, 1), (2, 3)), (0, 0, 0, 0)),
            (0, (3,)), ((0, 1), (1, 3)),
            (((0, 1), (2, 3)), (1, 2, 1, 2)),
            ((((0, 1), (2, 3)), ((4, 5), (6, 7))), (1, 0, 1, 1, 0, 1, 0, 1)),
            ((0, 1), (3, 3))]
for tree, populations in families:
    check_complete_feature_family(tree, populations)

# Locally valid metadata may have no companion at the prescribed root size.
for populations, caps, query, intersection, a, expected in [
        ((2, 2), (3, 2, 2), (0, 0), 0, 0, None),
        ((2, 2), (3, 2, 2), (0, 0), 1, 0, Fraction(0)),
        ((1, 2), (2, 1, 2), (1, 1), 0, 2, Fraction(1, 2))]:
    tree = (0, 1)
    h = accumulate_original_leaf_values(tree, populations)
    cap = {tree: caps[0], 0: caps[1], 1: caps[2]}
    core, r = compile_independent_additive_core(tree, h, cap, query)
    root = evaluate_independent_core_thresholds(core, Counter(), {})
    assert recover_exact_root_envelope(root, intersection, a) == expected
    checks["named_companion_cases"] += 1

scaling = []
for requested_r in (0, 1, 2, 4, 8, 16, 32):
    tree, row0, row1 = 0, [2], [0]
    for step in range(requested_r):
        positive, negative = len(row0), len(row0) + 1
        row0.extend((1, 0))
        row1.extend((0, 1))
        tree = (positive, (negative, tree))
    first = accumulate_original_leaf_values(tree, row0)
    second = accumulate_original_leaf_values(tree, row1)
    h = {v: first[v] + second[v] for v in first}
    cap = {v: max(first[v], second[v]) for v in first}
    query = [1] + [0] * (len(row0) - 1)
    core, r = compile_independent_additive_core(tree, h, cap, query)
    stats, retained = Counter(), {}
    root = evaluate_independent_core_thresholds(core, stats, retained)
    score = recover_exact_root_envelope(root, 0, 1)
    assert r == requested_r and score == Fraction(1, r + 2)
    scaling.append((r, len(row0), len(retained), stats["initialized_cells"], 2 ** r, str(score)))

assert {"r_zero", "adjacent_strict", "positive_A", "empty_bundle",
        "multiple_boundaries", "zero_query", "root_tie"} <= coverage
print("checks", dict(sorted(checks.items())))
print("coverage", sorted(coverage))
print("scaling_columns", ("r", "G", "core_nodes", "initialized_threshold_cells", "old_assignments", "score"))
for row in scaling:
    print("scaling", row)
print("negative_symbolic", {"r": 0, "Q": 10 ** 12, "dual_cells_not_allocated": 2 * (10 ** 12 + 1), "score": "1"})
print("result", "PASS")
```

Reproduction from the repository root, with output only to stdout:

```sh
sed -n '/^```python$/,/^```$/p' research_algorithms_20260920/Similarity-Compressed-Dual-Review.md | sed '1d;$d' | /Users/amuldotexe/.local/bin/python3.11 -B
```

## Executed Result

The retained independent checker exited successfully. Complete stdout follows;
there are no project imports, lead-suite runs, or timing measurements.

```text
checks {'actual_root_frontiers': 1241, 'infeasible_root_envelopes': 376, 'metadata_summaries': 47, 'named_companion_cases': 3, 'ordered_full_union_pairs': 1810, 'root_envelopes': 8050, 'subtree_inverse_values': 23954, 'subtree_threshold_entries': 22730, 'summary_query_cases': 1241}
coverage ['adjacent_strict', 'empty_bundle', 'multiple_boundaries', 'positive_A', 'r_zero', 'root_tie', 'zero_query']
scaling_columns ('r', 'G', 'core_nodes', 'initialized_threshold_cells', 'old_assignments', 'score')
scaling (0, 1, 1, 4, 1, '1/2')
scaling (1, 3, 4, 16, 2, '1/3')
scaling (2, 5, 7, 28, 4, '1/4')
scaling (4, 9, 13, 52, 16, '1/6')
scaling (8, 17, 25, 100, 256, '1/10')
scaling (16, 33, 49, 196, 65536, '1/18')
scaling (32, 65, 97, 388, 4294967296, '1/34')
negative_symbolic {'r': 0, 'Q': 1000000000000, 'dual_cells_not_allocated': 2000000000002, 'score': '1'}
result PASS
```

The 1241 summary/query configurations cover 47 metadata summaries from 1810
ordered full-union pairs, across six fixed layouts. They yield 22730 checked
subtree threshold entries, 23954 inverse values and 8050 root envelope checks;
376 of those root checks are correctly infeasible. All 1241 root frontiers also
match actual feature-set enumeration. Three named companion/feasibility cases
and seven Q=1 scaling cases pass. These are bounded corroboration of the proof,
not independent large datasets or a substitute for the proof.

The ladder initializes exactly 12r+4 threshold cells in this check, including
sequential-fold intermediate tables; this is not a measured peak-memory count.
At r=32 its 388 initialized cells contrast with 4294967296 assignments in the
specified old unconditional schedule. No such assignment list is generated.
The negative r=0,Q=10^12 reservation is arithmetic only: its 2000000000002
threshold cells are never allocated. A direct scalar formula supplies its
stated score.

## Lead Reports, Not Audited

The lead separately reports 29440 whole-envelope oracle cases, 240 reference
comparisons, and a green 116-test suite. Reported plan/state/work counters are
G=256,r=128: P_plan=513,S=2066,W=2860, and fixed r=1,G=4,...,256:
P_plan=5,S=34,W=66. These are not this review's executions, not added to its
counts, and not a comparison of identical counter definitions. The independent
structural identity V_plan=K+E_component supports the proposed expansion; it
does not certify those implementation counters or allocation behavior.

## Closure

**Accept the exact composition with the trusted-preparation, companion,
infeasibility, bit-width and numerical-Q qualifications above.** Use the uniform
bound O((r+1)(Q+1)+Q^2), with O((r+1)(Q+1)) temporary cells. No new min-plus or
generic DP primitive is claimed. Implementation correctness, actual retained
ownership, usefulness and publication priority remain unaudited.

Only `research_algorithms_20260920/Similarity-Compressed-Dual-Review.md` changed
in this theorem sidecar. No shared journal, other document, source, test,
receipt, commit or push was touched. The preceding workload profile remains
frozen with SHA-256
`34bdeafe23dc4003a080ce3f251eddba23fbddbd2755796bdefcf2407b829944`.

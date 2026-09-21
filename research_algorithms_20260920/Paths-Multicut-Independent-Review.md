# A03 Multicut: Independent Mathematical And Closest-Art Review

Date: 2026-09-21. Bounded review of `Paths-Multicut-Revival-Partition.md`;
the one-cut and partitioned-row prerequisites were consulted only for their
inherited contracts. Only this review file is owned. No project code audit,
lead-probe reuse, performance measurement, repository survey, or commit.

## Terminal Decision

**GO, conditional on the stated prepared-epoch/index contract.** No mathematical
counterexample was found to the multicut construction. The current-tail-owner
correction is essential, not optional. This approves the representation and
conditional logical work bounds, not an implementation or physical-memory claim.

**NO-GO for an unconditional novelty or superiority claim.** The three-sided
primitive is established; disjointifying laminar subtree unions is elementary;
an equally indexed canonical contraction can use precisely these regions and
match the bounds. Multiple-failure exact distances and original-path reporting
are not, by themselves, a new problem or capability.

| Claim challenged | Finding |
| --- | --- |
| Deepest-cut revival iff | GO for originally ancestor-dominated records only, with strict tail-depth inequality. |
| At most `2k` disjoint head bands | GO; sharper bound `2k-c0` for `k>0`, where `c0` is the number of top-level cut subtrees. Zero bands for `k=0`. |
| Current-tail-owner revival | GO; grouping by the old owner is demonstrably wrong under nested cuts. Preserve the actual raw head. |
| Old-ledger partition counts | GO: `I <= q+2t+ell`, `I <= A-ell`, and `Z_Q <= A-ell`. These are record/report counts, not page-read counts. |
| Exact original distance and simple output | GO for primary distance in the full raw graph and a simple original-ID path; minimum hops only in the retained graph. |
| Sparse publication and solve bounds | Conditional GO; preparation, metadata/ID access, source setup, arithmetic, output, and version retention remain paid. |

## Mathematical Challenges

### 1. Revival Predicate And Unique Reporting

Fix an omitted record `(x,y)` with `x` an original ancestor of `y`. Its unique
forest witness consists of incoming edges of vertices strictly after `x` on
that ancestor chain. It intersects `C` exactly when a cut child on the chain
has depth greater than `depth(x)`. Taking the deepest cut ancestor of `y`
preserves this existential condition. This proves both directions, including
multiple components, nested cuts, parallel edge identities, and `x=y`.

Two boundary counterexamples guard the statement:

- Chain `0->1->2->3`, cuts into `1` and `3`, omitted `1->3`: the deepest
  predicate reports it, but the shallowest-cut predicate misses it.
- Chain `0->1->2`, cut into `1`, omitted `1->2`: `<=` would spuriously report
  it. The witness starts below the failed edge. A loop at a cut child similarly
  must not revive.

This is witness invalidation, not proof that every reported edge is necessary
for a shortest path. Alternative live witnesses may make some reported records
redundant; charging their actual count `r` remains correct.

The cut-subtree inclusion forest has `k` nodes and `k-c0` inclusion edges.
Subtracting its immediate child intervals from a node interval yields at most
`1+child_count` pieces. Summing gives `2k-c0`, before removing empty pieces.
Every covered head lies in exactly one residual piece, belonging to its deepest
cut ancestor. Hence disjoint range reports enumerate each original record once.
Duplicate coordinates must retain distinct IDs, and a half-open subtree bound
must include or exclude an entire head-coordinate tie group.

The chain with `k` cuts and `r` parallel root-to-bottom alternatives separates
this plan from **independent per-cut reporting**, which emits `kr` occurrences.
It does not separate it from a generic union reporter allowed the same bands.

### 2. Current Ownership, Holes, And Counts

On `0->1->2->3`, delete `0->1` and `2->3`, and revive raw `1->3`.
With old gate set `{0}`, current entries include `1` and `3`. Its macro is
`1->3`, not `0->3`: the latter invents the deleted prefix `0->1`.
A source inside a current region can split its revived-tail group again.
Transfer the matching records to the source region; do not retain a duplicate
old-owner macro. The actual head `3` cannot be replaced by an ancestor root.

Every new mark, including a detached cut root and an optional source, has one
strict inclusion parent within its old capsule. It contributes one hole even
when its incoming connector is deleted. Original capsule boundaries already
handle cut children that are old gates. Thus there are `q+t` regions and `t`
holes. Before tombstones, the piece count is at most `q+2t`. Removing `ell`
distinct ledger ranks adds at most `ell` pieces. Every nonempty final piece
contains a surviving record, and pieces are disjoint, giving `I<=A-ell`.

The canonical tail owner is the deepest current entry ancestor. A cut on its
prefix would have a deeper cut-child entry, a contradiction. Removing only
immediate marked-child intervals therefore gives exactly its live tail domain.
Deleting a cut root's hole because its connector is absent breaks this proof.
Deleting an endpoint pair instead of a unique forest-record rank wrongly
deletes parallel survivors.

Inject each interval/head report into that head's first record in that exact
interval. The injection proves `Z_Q<=A-ell`, provided each settled state is
enumerated once. Minima must be clipped to that same interval. An RMQ over the
unsubtracted ancestor interval can pick a cheaper record across a failed cut.
No statement here bounds shared index-node visits, disk pages, or repeat queries.

### 3. Original Distance And Simple Lifting

Two distinct equivalences are needed:

1. For every omitted arc outside `R`, its entire original forest witness is
   live and has reduced cost zero, no larger than the omitted arc's cost.
   Replacing such arcs by witnesses and using the retained-subgraph inclusion
   proves equality of **primary** distances between raw and retained graphs.
2. In the retained graph, all nonforest heads and all component roots are
   entries. Canonical regions and live connectors simulate every path segment.
   Each emitted macro conversely lifts to an original live segment. Clipped
   minima preserve lexicographic optimum because, for fixed owner `g`, their
   secondary length is `depth(tail)-depth(g)+1`.

Consequently the quotient preserves `(reduced cost, original hops)` in the
retained graph. For any target, head closure forces the final entry to be its
current owner; the forest suffix contributes zero reduced cost and a fixed
hop count. Restore original cost by `h(target)-h(source)`.

A repeated original vertex in a lifted lexicographically minimum retained walk
would enclose a cycle. Its reduced cost is nonnegative; deleting it either
reduces cost or preserves cost and reduces positive original hops. Both
contradict optimality. This argument includes the final suffix and zero cycles;
merely knowing that the **state** predecessor chain is acyclic would not suffice.

Do not strengthen this to raw-graph minimum hops. With no cuts, forest
`0->1->2` and an omitted zero-cost raw `0->2`, the retained shortest path uses
two hops while the full raw graph permits one. Both have exact primary cost.
Validate all original edge identities/incarnations against the pinned epoch;
an endpoint-equal alternative is not the deleted selected identity.

## Costs And Assumptions

The displayed bounds survive the following accounting checks:

- `O(k+r)` new retained words describe cuts/bands, unique revivals, marks,
  inclusion links, owner events and changed row pieces. Unchanged rows must
  remain implicit defaults. Allocating fresh descriptors for all `q` rows
  invalidates that publication bound, although `q` solve states are paid.
- Sort cuts/marks/records and build the sparse inclusion structures explicitly.
  The stated `(k+r)log(k+r+2)` term covers classification and ordering; rank
  endpoint searches and connector tombstones use `(k+r)log(A+2)`. No walk down
  all affected descendants is justified by these bounds.
- Unit metadata access presupposes paid vertex-to-coordinate/old-owner tables,
  selected-forest identity lookup, and connector-ID-to-ledger-rank access.
  Otherwise add the actual lookup/join cost. The note correctly calls out a
  possible additional `k log n`; other non-unit metadata joins are also paid.
- The `k log(D+2)+r` reporting term requires the prepared output-sensitive
  geometric index. The strong `I_Q+Z_Q` color term requires the declared
  constant-query-time static RMQ profile, not merely a balanced aggregate tree.
  Their base construction/storage do not disappear from end-to-end cost.
- A source resweep may inspect `O(k+r)` descriptors, not all vertices or ledger
  records. An indexed heap is needed for the stated mutable-state bound; a lazy
  duplicate heap may retain additional edge-dependent entries. Target lookup,
  witness reads, forward spooling and complete output stay outside that bound.
- Exact integer/fixed-point sums, depths and IDs must fit the declared word
  model, or their bit-operation costs must be charged. No floating-point tie
  approximation, overflow, invalid potential, nonforest edit, or changed epoch
  is covered. Potential feasibility is preserved by deletion alone.
- The input batch is validated distinct selected-forest identities. Repeated
  cumulative publications repay discovery/setup for the whole cumulative cut
  set. A zero-cut publication needs only a constant empty-version header;
  write `O(1+k+r)` if literal empty-case allocation is part of the API.

These are conditions on the theorem, not measured evidence that an engine
implements it. Large `r`, `q`, or `A` can remove any practical reduction.

## Bounded Primary Closest Art

**Inspected scope is intentionally limited.** The PDFs below were reachable as
parsed primary full text, but only the listed portions were inspected; their
remaining proofs were not audited. No citation-chain expansion was performed.

| Primary source | Inspected scope and relevant comparison |
| --- | --- |
| Vassilevska Williams, Woldeghebriel, Xu, [Algorithms and Lower Bounds for Replacement Paths under Multiple Edge Failures, arXiv:2209.07016v1](https://arxiv.org/pdf/2209.07016) | Abstract and introduction through Theorem 1.3 and the path-reporting remark, PDF pages 1-4. Directed weighted graphs, fixed `s,t`, arbitrary edge failures; Theorem 1.1 gives a near-cubic two-failure algorithm and Corollary 1.2 a higher-failure extension. Theorem 1.3 is a preprocessing/query tradeoff. Most presented algorithms support output-sensitive path reporting, with an explicit exception. This is broader failure coverage, not the same sparse published forest-cut interface. Neither their hardness discussion nor their storage bounds can be transplanted as a lower bound or forced baseline for A03. |
| Duan, Ren, [Maintaining Exact Distances under Multiple Edge Failures, arXiv:2111.03360v1](https://arxiv.org/pdf/2111.03360) | Abstract, introduction/Theorem 1.1, and Section 2 overview, PDF pages 1-6. An undirected weighted-graph oracle accepts arbitrary endpoint pairs and edge-failure sets, with stated `O(d n^4)` space and `d^{O(d)}` query dependence; preprocessing is not claimed efficient in `d`. Its overview uses replacement-path decomposition and hitting sets. A03 instead permits a new reduced-graph solve and failures only in a selected zero-cost directed forest. These are different tradeoffs and graph classes, not a demonstrated A03 improvement over that oracle. |
| McCreight, [Priority Search Trees, DOI 10.1137/0214021](https://doi.org/10.1137/0214021) | Publisher abstract and bibliographic metadata inspected. The PDF URL redirected to the abstract/access page; full proof not inspected. The abstract explicitly gives linear space and `O(log n+s)` reporting for pairs satisfying a bounded horizontal interval and an upper vertical threshold. This is precisely the imported three-sided reporting profile after encoding stable identity ties. The subtree-to-interval and disjoint-band reductions here are reviewer-derived elementary reductions, not attributed as a new geometry result. |

**Equal-index canonical contraction is the decisive internal comparator.** Give
it the same forest/potential, raw identities, revival reporter, old row order,
color index, clipped minima, snapshot and full output requirements. It can use
the same marked holes, current-owner grouping and source split. Its state and
report bounds then match A03. This is a constructive comparison, not a claim
that a particular historical implementation used every detail.

The supportable contribution candidate is therefore the explicit integration
contract: one immutable epoch, unique multicut witness-revival reporting,
current-tail-correct canonical regions, and sparse publication without owner
rewrites. Correct composition does not establish historical priority for that
integration; the bounded inspected literature neither proves nor disproves it.

## Standalone Independent Exact Checker

This checker uses only the Python standard library. It imports no project code
and reads no project state. All functions below are independently written.
The construction uses tiny explicit sets/scans, **not** the proposed sparse
publisher, color index or geometric data structure. Bellman-Ford over original
weights is the independent distance oracle; quotient solving uses lexicographic
Dijkstra and verifies every lifted original-ID path.

It exhausts all topologically numbered rooted forests on 1-4 vertices, every
old-gate superset of the roots, every forest-cut subset, and every source and
target, for the deterministic multigraph family generated below. Each graph
contains ancestor shortcuts, parallel alternatives, loops, and nonancestor
arcs into old gates. Reduced costs include zero and positive values; nonconstant
positive potentials also exercise negative original edges without negative
cycles. This is not exhaustion of all weighted directed multigraphs.
Two additional sparse fixtures (five and six vertices) cover unreachable
targets, an isolated root, a dormant return, nested/disjoint cuts, and a
cross-capsule cut with an endpoint-equal surviving raw alternative.

Run without creating another file, from the repository root:

```sh
awk '/^```python$/{p=1;next} p && /^```$/{exit} p{print}' research_algorithms_20260920/Paths-Multicut-Independent-Review.md | python3 -B -
```

```python
from collections import Counter, defaultdict
from heapq import heappop, heappush
from itertools import product


def enumerate_all_subset_masks(values):
    values = sorted(values)
    for bits in product((False, True), repeat=len(values)):
        yield {v for v, present in zip(values, bits) if present}


def derive_forest_vertex_metadata(parent):
    chains, order = [], []
    for v, p in enumerate(parent):
        chains.append((chains[p] if p >= 0 else ()) + (v,))

    def visit_original_forest_vertex(v):
        order.append(v)
        for w, p in enumerate(parent):
            if p == v:
                visit_original_forest_vertex(w)

    for v, p in enumerate(parent):
        if p < 0:
            visit_original_forest_vertex(v)
    tin = {v: i for i, v in enumerate(order)}
    end = {v: tin[v] + sum(v in chain for chain in chains)
           for v in range(len(parent))}
    depth = {v: len(chain) - 1 for v, chain in enumerate(chains)}
    return chains, tin, end, depth


def walk_original_forest_edges(x, y, chains):
    assert x in chains[y]
    # A selected forest edge's ID is its child vertex.
    return chains[y][len(chains[x]):]


def solve_exact_reference_labels(n, edges, source, potential):
    labels = [None] * n
    labels[source] = (0, 0)
    for _ in range(n - 1):
        changed = False
        for x, y, cost in edges.values():
            if labels[x] is None:
                continue
            candidate = (labels[x][0] + cost - potential[x] + potential[y],
                         labels[x][1] + 1)
            if labels[y] is None or candidate < labels[y]:
                labels[y], changed = candidate, True
        if not changed:
            break
    return labels


def solve_quotient_original_paths(macros, source):
    labels, paths, queue = {source: (0, 0)}, {source: ()}, [(0, 0, source)]
    while queue:
        cost, hops, x = heappop(queue)
        if labels[x] != (cost, hops):
            continue
        for y, reduced, witness in macros[x]:
            candidate = (cost + reduced, hops + len(witness))
            if y not in labels or candidate < labels[y]:
                labels[y] = candidate
                paths[y] = paths[x] + witness
                heappush(queue, (*candidate, y))
    return labels, paths


def run_independent_multicut_check(parent, gates, cut, edges, omitted,
                                   ledger, metadata, counts):
    chains, tin, end, depth = metadata
    n = len(parent)
    old = {v: max(gates.intersection(chains[v]), key=depth.get)
           for v in range(n)}
    bands = []
    for c in cut:
        children = [z for z in cut if z != c and c in chains[z]
                    and not any(w != c and w != z and c in chains[w]
                                and w in chains[z] for w in cut)]
        cursor = tin[c]
        for z in sorted(children, key=tin.get):
            if cursor < tin[z]:
                bands.append((cursor, tin[z], depth[c]))
            cursor = end[z]
        if cursor < end[c]:
            bands.append((cursor, end[c], depth[c]))
    top = sum(not (cut.intersection(chains[c][:-1])) for c in cut)
    assert len(bands) <= 2 * len(cut) - top
    coverage = Counter(i for lo, hi, _ in bands for i in range(lo, hi))
    assert coverage == Counter({tin[v]: 1 for v in range(n)
                                if cut.intersection(chains[v])})
    expected = {e for e in omitted
                if cut.intersection(walk_original_forest_edges(
                    edges[e][0], edges[e][1], chains))}
    reports = [e for lo, hi, threshold in bands for e in omitted
               if lo <= tin[edges[e][1]] < hi and depth[edges[e][0]] < threshold]
    assert Counter(reports) == Counter({e: 1 for e in expected})
    revived = set(reports)
    repeated = sum(c in chains[edges[e][1]] and depth[edges[e][0]] < depth[c]
                   for c in cut for e in omitted)
    counts['independent_rectangles_duplicate'] += repeated > len(revived)
    for e in omitted:
        x, y, _ = edges[e]
        ancestors = cut.intersection(chains[y])
        deepest = max(ancestors, key=depth.get) if ancestors else None
        correct = deepest is not None and depth[x] < depth[deepest]
        assert correct == (e in expected)
        shallow = min(ancestors, key=depth.get) if ancestors else None
        counts['shallowest_wrong'] += bool(shallow is not None and
                                          depth[x] < depth[shallow]) != correct
        counts['nonstrict_wrong'] += bool(deepest is not None and
                                         depth[x] <= depth[deepest]) != correct
    new = (cut | {edges[e][1] for e in revived}) - gates
    assert len(new) <= len(cut) + len(revived)
    live = {e: arc for e, arc in edges.items() if e not in cut}
    retained = {e: arc for e, arc in live.items() if e not in omitted - revived}
    rows = {g: sorted((e for e in ledger if old[edges[e][0]] == g),
                       key=lambda e: (tin[edges[e][0]], e)) for g in gates}
    ell = len(ledger & cut)
    potential = [(3 * v) % 5 + 1 for v in range(n)]
    for source in range(n):
        entries = gates | new | {source}
        marks = entries - gates
        owner = {v: max(entries.intersection(chains[v]), key=depth.get)
                 for v in range(n)}
        parents = {m: max(entries.intersection(chains[m][:-1]), key=depth.get)
                   for m in marks}
        macros, seen = defaultdict(list), Counter()
        intervals = colors = 0
        for g in entries:
            row = rows[old[g]]
            holes = [m for m in marks if parents[m] == g]
            selected = [i for i, e in enumerate(row) if e not in cut
                        and g in chains[edges[e][0]]
                        and not any(m in chains[edges[e][0]] for m in holes)]
            runs = []
            for i in selected:
                if not runs or i != runs[-1][-1] + 1:
                    runs.append([])
                runs[-1].append(i)
                e = row[i]
                seen[e] += 1
                assert owner[edges[e][0]] == g
            intervals += len(runs)
            for run in runs:
                heads = {edges[row[i]][1] for i in run}
                colors += len(heads)
                for y in heads:
                    e = min((row[i] for i in run if edges[row[i]][1] == y),
                            key=lambda e: (edges[e][2], depth[edges[e][0]], e))
                    x, _, cost = edges[e]
                    witness = walk_original_forest_edges(g, x, chains) + (e,)
                    assert not cut.intersection(witness)
                    macros[g].append((y, cost, witness))
        assert seen == Counter({e: 1 for e in ledger - cut})
        assert intervals <= min(len(gates) + 2 * len(marks) + ell, len(ledger) - ell)
        assert colors <= len(ledger) - ell
        for m, g in parents.items():
            if m not in cut:
                witness = walk_original_forest_edges(g, m, chains)
                assert witness and not cut.intersection(witness)
                macros[g].append((m, 0, witness))
        for e in revived:
            x, y, cost = edges[e]
            counts['old_owner_broken'] += bool(cut.intersection(
                walk_original_forest_edges(old[x], x, chains)))
            witness = walk_original_forest_edges(owner[x], x, chains) + (e,)
            assert not cut.intersection(witness)
            macros[owner[x]].append((y, cost, witness))
        labels, paths = solve_quotient_original_paths(macros, source)
        raw = solve_exact_reference_labels(n, live, source, potential)
        ref = solve_exact_reference_labels(n, retained, source, potential)
        for target in range(n):
            g = owner[target]
            suffix = walk_original_forest_edges(g, target, chains)
            assert not cut.intersection(suffix)
            counts['target_checks'] += 1
            if g not in labels:
                assert raw[target] is None and ref[target] is None
                counts['unreachable_targets'] += 1
                continue
            result = (labels[g][0] + potential[target] - potential[source],
                      labels[g][1] + len(suffix))
            assert result == ref[target] and result[0] == raw[target][0]
            counts['raw_hop_gap'] += raw[target][1] < result[1]
            walk, vertex, visited, cost = paths[g] + suffix, source, {source}, 0
            for e in walk:
                assert e in live
                x, y, reduced = live[e]
                assert x == vertex and y not in visited
                visited.add(y)
                vertex = y
                cost += reduced - potential[x] + potential[y]
            assert vertex == target and (cost, len(walk)) == result
            counts['lifted_paths'] += 1
        counts['source_solves'] += 1
    counts['forest_gate_cut_cases'] += 1


def check_forest_multigraph_family(parent, gates, arcs, counts):
    metadata = derive_forest_vertex_metadata(parent)
    chains, tin, end, depth = metadata
    forest = {v for v, p in enumerate(parent) if p >= 0}
    old = {v: max(gates.intersection(chains[v]), key=depth.get)
           for v in range(len(parent))}
    edges = {v: (parent[v], v, 0) for v in forest}
    omitted, ledger = set(), {v for v in forest if old[parent[v]] != old[v]}
    for e, (x, y, cost) in enumerate(arcs, len(parent)):
        assert cost >= 0 and (x in chains[y] or y in gates)
        edges[e] = (x, y, cost)
        bucket = omitted if x in chains[y] and not (x == y and y in gates) else ledger
        bucket.add(e)
    for cut in enumerate_all_subset_masks(forest):
        run_independent_multicut_check(parent, gates, cut, edges, omitted,
                                      ledger, metadata, counts)


def run_bounded_exact_review():
    counts = Counter()
    for n in range(1, 5):
        for parent in product(*(range(-1, v) for v in range(n))):
            chains, _, _, _ = derive_forest_vertex_metadata(parent)
            roots = {v for v, p in enumerate(parent) if p < 0}
            for extra in enumerate_all_subset_masks(set(range(n)) - roots):
                gates, arcs = roots | extra, []
                for x in range(n):
                    for y in range(n):
                        ancestor = x in chains[y]
                        if ancestor or y in gates:
                            base = 0 if ancestor else (x + 2 * y) % 3
                            arcs.extend(((x, y, base), (x, y, base + 1)))
                check_forest_multigraph_family(parent, gates, arcs, counts)
    check_forest_multigraph_family(
        (-1, 0, 1, 2, -1), {0, 4}, [(1, 3, 0), (3, 0, 0)], counts)
    check_forest_multigraph_family(
        (-1, 0, 1, 1, 0, -1), {0, 2, 5},
        [(0, 3, 2), (1, 3, 0), (3, 0, 0), (4, 2, 1),
         (4, 5, 3), (2, 2, 0), (1, 2, 0)], counts)
    for mutation in ('shallowest_wrong', 'nonstrict_wrong', 'old_owner_broken',
                     'independent_rectangles_duplicate', 'raw_hop_gap',
                     'unreachable_targets'):
        assert counts[mutation] > 0, mutation
    print('PASS', dict(sorted(counts.items())))


if __name__ == '__main__':
    run_bounded_exact_review()
```

### Execution Record

The command above completed with exit status 0. Exact output:

```text
PASS {'forest_gate_cut_cases': 660, 'independent_rectangles_duplicate': 130, 'lifted_paths': 10176, 'nonstrict_wrong': 1093, 'old_owner_broken': 699, 'raw_hop_gap': 237, 'shallowest_wrong': 339, 'source_solves': 2622, 'target_checks': 10562, 'unreachable_targets': 386}
```

The 10,562 target checks comprise 10,176 validated simple original-ID paths
and 386 confirmed unreachable results. Wrong-predicate/report counters count
detected disagreements or broken prefixes, not separate mutation-engine runs.
The raw-hop-gap counter confirms why no full-raw minimum-hop claim is made.

The general proof and primary-source scope are independent of the finite
checker; finite success cannot establish a universal theorem, data-structure
cost bound, sparse-memory implementation, or novelty. The lead's separate
reference probe was neither read nor used. **Bounded review complete; terminal
conditional GO with the NO-GO claim boundaries above.**

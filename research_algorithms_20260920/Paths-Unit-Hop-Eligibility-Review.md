# A03 Unit-Hop Eligibility: Independent Review

Date: 2026-09-21. Bounded mathematical and primary closest-art review under
the [multicut contract](Paths-Multicut-Independent-Review.md). Only this file
is owned. No production-code inspection, source-graph scan/profile, lead-probe
reuse, measurements, other file edits, or commit.

## Decision

**GO for both proposed theorems, with the qualifications below.** No
counterexample exists under the stated finite, simple, loopless, directed,
unit-weight graph and outward-forest contract; the proofs below establish this
independently of the finite checker.

```text
D = r = 0
q_min = n - n1 + c1
```

Here indegree counts all original incoming edges, not selected-forest degree;
`c1` counts directed cycles whose every vertex has original indegree one.
These cycles are vertex-disjoint. The optimization is over all admissible
`F,h`, with gates exactly forest roots plus heads of nonforest edges.
It is not an optimization over all possible shortest-path representations.

**NO-GO** for silently applying this to raw parallel identities, admitting
loops, using the forward orientation's degree counts for reverse queries,
seeding an interior source at its old owner, or claiming generic contraction
novelty, universally optimal preprocessing, or a shortest-path lower bound.

## Independent Proof

### 1. No Omitted Ancestor Edges

Use `c(u,v)=1+h(u)-h(v)>=0`. A directed path of `L` selected forest edges
has `h(v)-h(u)=L` because every selected edge has reduced cost zero. If a raw
edge `(u,v)` has `u` a forest ancestor of `v`, feasibility gives `1-L>=0`.
Looplessness gives `L>=1`; therefore `L=1`. The forest already contains an
edge with those endpoints, and simplicity makes it the same identity.
Consequently no **nonforest** raw edge is ancestor-dominated. Thus `D=0`,
and any selected-forest deletion batch has `r=0` in this prepared epoch.

The argument permits arbitrary real feasible potentials and arbitrary cycles
in the original graph. Only the selected forest is acyclic. It does not assume
that `h` was computed by an anchor shortest-path search.

### 2. Exact Non-Gate Characterization

Since no nonforest edge is omitted, all its heads must be gates. A non-gate
is not a forest root and has no nonforest incoming edge. Its sole incoming
edge is therefore its selected parent edge: original indegree is exactly one.
Conversely, a vertex of original indegree one whose incoming edge is selected
is neither a root nor a nonforest head, hence is a non-gate. In particular,

```text
q(F,h) = n - number of indegree-one vertices with their incoming edge in F.
```

An indegree-zero vertex is a root. An indegree-at-least-two vertex is a gate
even if a selected edge enters it, because at most one incoming edge can be
selected. Extra prescribed gates or a fixed potential change the optimization
problem and are not covered by the attainment claim.

### 3. Lower Bound And Attainment

Let `V1` be the original indegree-one vertices. Each vertex of `V1` has a
unique predecessor. Distinct directed cycles entirely in `V1` cannot share a
vertex: tracing predecessors from a shared vertex fixes the entire cycle.
Every selected forest must omit at least one incoming edge on each such cycle.
It can therefore make at most `n1-c1` of these vertices non-gates, proving
`q >= n-n1+c1`.

For attainment, let `H` contain every edge whose head lies in `V1`. Its only
possible directed cycles are exactly those counted by `c1`. Delete one edge
from each cycle to obtain `F`. It has at most one incoming edge per vertex
and no directed cycles, hence is an outward rooted forest, including isolated
vertices. Its roots are precisely `V-V1` and one broken-cycle head per cycle.

Set `h(v)=depth_F(v)`, with every root at depth zero. Selected edges have
reduced cost zero. Every other original edge enters either `V-V1` or a
broken-cycle head, both roots, so its reduced cost is `1+depth_F(tail)>=1`.
Thus this potential is feasible, all nonforest heads are already roots, and
`P=roots(F)`, with `q=n-n1+c1`. This proves a global minimum within the stated
quotient without anchor SSSP.

Count cycles in `H`, not every cycle/SCC of the original graph. For example,
`0->1`, `1->0`, `2->0` has `n1=1,c1=0`, and admits `q=2`; its original
directed cycle needs no additional cycle correction because `0` is already
an indegree-two gate. Multiple disjoint cycles and isolated roots simply add
their respective contributions. The empty graph has `q_min=0`.

The depth construction is **not** a feasible potential for every arbitrary
forest. For `F={0->1,1->2}` plus raw `3->2`, forest depths give reduced cost
`-1` to `3->2`; assigning `h(3)=1` fixes feasibility. Attainment uses the
specific forest above, not this invalid generalization.

## Query, Witness, And Cut Consequences

For any admissible `F,h`, the earlier canonical quotient proof applies with no
revivals. For the attaining construction there are further simplifications:

- `P` consists exactly of forest roots, so no selected forest edge crosses old
  capsules. The complete old ledger consists of `E-F`, including broken-cycle
  returns to their own capsule roots. Its size is `A=m-n1+c1`.
- A batch of `k` distinct selected identities has `k` distinct nongate children.
  The standard entry-closed representation adds **exactly `k`** roots here,
  versus at most `k` for an arbitrary admissible forest. There are no old-ledger
  connector tombstones (`ell=0`) for this construction. Prefix restrictions
  still matter: rows below cuts cannot be used from their former owner.
- Each cut removes its child's sole original incoming edge. Therefore each new
  cut root has indegree zero in the surviving raw graph. From a fixed source,
  no such root other than the source itself is reachable. Thus at most `q+1`
  entries can actually be reached, although an implementation materializing
  all entries may still allocate `q+k+sigma` states. This refinement applies
  to the attaining construction, not every admissible `F`.

For arbitrary source `s`, promote `s` if needed. Let `rho_s(v)` be the deepest
entry ancestor of `v` along the surviving forest, with entries consisting of
old roots, cut children and `s`. A macro for ledger identity `e=(x,y)` starts
at `g=rho_s(x)`, follows the unique live forest prefix `g->x`, then uses the
actual raw edge to the actual head `y`. Its reduced cost is `c(e)` and its
original length is `depth(x)-depth(g)+1`. Source-promotion connectors have
reduced cost zero and their actual forest-path lengths.

With source-seeded reduced labels `D`,

```text
dist(s,v) = D(rho_s(v)) + h(v) - h(s),
```

provided an unreachable owner remains explicitly unreachable. Recover the
predecessor macros and append the live `rho_s(v)->v` suffix. Witnesses retain
original IDs/incarnations and parent-edge IDs; no new shortcut ID substitutes
for an original edge in delivered output.

**Interior-source counterexample to the wrong interface:** on the outward star
`0->1,0->2`, starting a query for source `1` at old root `0` falsely makes `2`
reachable. Potential subtraction cannot repair this false reachability. Source
promotion must split the actual tail domain; an old whole-row minimum may
also select an edge from an ancestor or sibling outside the source's domain.

Here retained and original graphs coincide after cuts. Original cost equals
hop count, so exact primary distances also give **full-original minimum hops**,
unlike the more general dominated-shortcut contract. Every original directed
cycle has strictly positive cost, hence so does its reduced-cost sum. A
minimum lifted walk cannot repeat an original vertex. Lexicographic Dijkstra
remains valid, but the extra hop coordinate is unnecessary for this unit-cost
simplicity argument. The compressed graph is weighted: a macro spanning
several original edges must not be treated as one original hop.

## Boundaries And Lookup Obligations

**Direction reversal.** Apply the theorem anew to `G^R`: its `n1` is the count
of original outdegree-one vertices, and its `c1` counts cycles wholly among
those vertices. The star `0->1,0->2,0->3` has forward `q_min=1` and reverse
`q_min=4`. Reversing an outward branching forest generally violates the
one-incoming-selected-edge condition. Rebuild the forest, coordinates,
potential and owner interpretation in the requested traversal direction.

**Parallel identities.** Two unit edges `e,f:0->1`, with `e` selected and
`h=(0,1)`, make `f` ancestor-dominated. Deleting `e` revives `f`; `D=r=1`.
Deduplicating endpoint pairs preserves static unweighted vertex distances,
but not raw-identity deletion semantics: deleting one representative must not
delete surviving alternatives. A normalized simple-graph study must explicitly
state the normalization and either restrict failures to normalized edges or
retain and charge separate multiplicity/provenance machinery. Do not silently
substitute distinct-predecessor count for raw indegree.

**Loops and weights.** A unit self-loop is dominated by the empty forest path,
so admitting it invalidates `D=0` (although this loop never needs cut revival).
A weighted triangle `0->1` of cost 1, `1->2` of cost 1, and `0->2` of cost 2
also admits a dominated nonforest shortcut with `h=(0,1,2)`. Neither is within
the theorem. Antiparallel edges with distinct endpoints are allowed; a directed
two-cycle contributes one to `c1` if both original indegrees are one.

**Paid access and witnesses.** The formula uses original indegrees, unique
predecessors and cycle detection; reading that topology remains preparation.
Avoiding anchor SSSP does not avoid input processing or index construction.
The precise query interface still needs:

- Paid vertex/edge ID lookup, forest parent IDs, root owner, depth and ancestry
  coordinates. Without a root-owner table/index, climbing parents can inspect
  an entire capsule just to resolve one original target.
- For cuts, a deepest-mark lookup in immutable preorder coordinates, e.g.
  `O(log(k+2))` lookup in a prepared sparse interval-event representation, plus
  the old-owner lookup. Source override is valid only below `s` with no cut on
  its forest prefix. No rewritten vertex-owner plane is necessary.
- Tail-ranked old ledger rows, actual-head discovery and minima clipped to
  current regions if the previous output-sensitive row-access bounds are
  claimed. `D=0` removes the omitted-arc revival index, not these row indexes.
  An unindexed implementation can scan, but must charge that scan.
- Real witness expansion, edge membership checks, original target delivery
  and any forward-order spool. Small `q` does not imply small raw metadata or
  short output. Direct-array lookup is a model assumption; string-key lookup,
  joins and external storage access have their own costs.

The minimum `q` concerns the old preparation only. It does not minimize total
query work, output, index size, source-dependent reached states, or the number
of states in an unrelated contraction/elimination scheme. The existing-source
eligibility study must establish its own orientation and simple/unit/loopless
input contract; this review makes no claim about that source's contents.

## Bounded Primary Closest Art

The search was limited to directed indegree-one elimination/preprocessing and
one primary shortest-path contraction comparator. No citation-chain survey or
performance comparison was made. Only the portions listed below were inspected.

| Primary source | Inspected scope and relevance |
| --- | --- |
| Dietzfelbinger and Jaberi, [On testing single connectedness in directed graphs and some related problems, arXiv:1412.1639v2](https://arxiv.org/pdf/1412.1639) | Abstract, introduction and Section 2 through Lemma 2.1 and its proof, PDF pages 1-4. The paper explicitly eliminates a directed indegree-one vertex by merging it with its predecessor and redirecting outgoing edges; it treats outdegree-one vertices via reversal. This is direct prior art for the elimination operation. Its setting after SCC reduction is acyclic, and its preserved property is single connectedness, not weighted distances or the A03 source/target interface. Its SCC contraction and treatment of parallel edges must not be imported as distance-preserving rules. |
| Geisberger, Sanders, Schultes and Delling, [Contraction Hierarchies: Faster and Simpler Hierarchical Routing in Road Networks, WEA 2008](https://ai.dmi.unibas.ch/research/reading_group/geisberger-et-al-wea2008.pdf) | Original paper at an academic mirror: introduction, Section 2's contraction definition and Section 4's Outputting Paths paragraph, PDF pages 1-4 and 7. It preserves shortest distances by weighted shortcuts and stores unpacking information. This is established general shortest-path vertex elimination and original-path recovery, not the particular indegree-one cycle-count formula. The inspected text does not establish that formula's historical priority or the sparse forest-cut interface. No experiments were used in this review. |

The appropriate characterization is an elementary **eligibility and optimal
gate-count corollary for this quotient**, with a constructive feasible potential
and explicit source/cut interface. Known indegree-one elimination, shortcut
contraction, and witness recovery deserve credit. The bounded sources do not
justify calling the generic operation new, nor do they settle historical
priority for the exact formula or its A03 formulation.

## Standalone Exact Checker

Python standard library only; no project imports or source-file reads. It
exhausts simple loopless directed graphs on 0-4 labeled vertices. For every
graph it enumerates all possible selected-parent choices, rejects directed
cycles, and tests potential existence with exact difference constraints:
`h(v)<=h(u)+1` on every raw edge and `h(u)<=h(v)-1` on selected edges.
This searches admissible forests without assuming forest depth is a generally
valid potential or restricting potentials to an arbitrary finite value range.

Cycle counting independently enumerates simple cycles among original
indegree-one vertices. For the proposed attaining forest it tests every cut
subset, source and target, comparing the explicit quotient and original-ID
path lifts to raw BFS. A six-vertex fixture adds two separate cycles and an
isolated root. Reversal is covered by exhaustive graph enumeration and an
explicit asymmetric star. Out-of-contract parallel/loop examples are also
checked. This is a correctness reference, not a sparse publisher or index.

Run from the repository root without creating another file:

```sh
awk '/^```python$/{p=1;next} p && /^```$/{exit} p{print}' research_algorithms_20260920/Paths-Unit-Hop-Eligibility-Review.md | python3 -B -
```

```python
from collections import Counter, defaultdict, deque
from heapq import heappop, heappush
from itertools import combinations, permutations, product


def enumerate_all_subset_choices(values):
    values = tuple(sorted(values))
    for size in range(len(values) + 1):
        for chosen in combinations(values, size):
            yield set(chosen)


def derive_forest_ancestor_chains(n, forest):
    parent = dict((v, u) for u, v in forest)
    if len(parent) != len(forest):
        return None
    chains = []
    for v in range(n):
        path, seen = [], set()
        while v not in seen:
            path.append(v)
            seen.add(v)
            if v not in parent:
                chains.append(tuple(reversed(path)))
                break
            v = parent[v]
        else:
            return None
    return chains


def find_exact_feasible_potential(n, edges, forest):
    constraints = [(u, v, 1) for u, v in sorted(edges)]
    constraints += [(v, u, -1) for u, v in sorted(forest)]
    h = [0] * n
    for _ in range(n):
        changed = False
        for u, v, bound in constraints:
            if h[v] > h[u] + bound:
                h[v], changed = h[u] + bound, True
        if not changed:
            return h
    return None if n else h


def build_unit_eligibility_forest(n, edges):
    incoming = {v: [u for u in range(n) if (u, v) in edges] for v in range(n)}
    single = {v for v in range(n) if len(incoming[v]) == 1}
    cycles = []
    for length in range(2, len(single) + 1):
        for cycle in permutations(sorted(single), length):
            if cycle[0] == min(cycle) and all(
                    (cycle[i - 1], cycle[i]) in edges for i in range(length)):
                cycles.append(cycle)
    assert sum(map(len, cycles)) == len({v for cycle in cycles for v in cycle})
    forest = {(incoming[v][0], v) for v in single}
    for cycle in cycles:
        v = min(cycle)
        forest.remove((incoming[v][0], v))
    chains = derive_forest_ancestor_chains(n, forest)
    assert chains is not None
    h = [len(chain) - 1 for chain in chains]
    roots = {chain[0] for chain in chains}
    expected = n - len(single) + len(cycles)
    assert roots == ({v for v in range(n)} - {v for _, v in forest})
    assert len(roots) == expected and all(v in roots for _, v in edges - forest)
    assert all(1 + h[u] - h[v] >= 0 for u, v in edges)
    assert all(1 + h[u] - h[v] == 0 for u, v in forest)
    assert all(u not in chains[v] for u, v in edges - forest)
    return forest, chains, h, roots, expected, incoming


def verify_minimum_gate_count(n, edges, expected, incoming, counts):
    best = n
    for parents in product(*[(-1, *incoming[v]) for v in range(n)]):
        forest = {(u, v) for v, u in enumerate(parents) if u >= 0}
        chains = derive_forest_ancestor_chains(n, forest)
        if chains is None:
            continue
        h = find_exact_feasible_potential(n, edges, forest)
        if h is None:
            counts['infeasible_forests'] += 1
            continue
        counts['admissible_forests'] += 1
        assert all(1 + h[u] - h[v] >= 0 for u, v in edges)
        assert all(1 + h[u] - h[v] == 0 for u, v in forest)
        assert all(u not in chains[v] for u, v in edges - forest)
        gates = {v for v, u in enumerate(parents) if u < 0}
        gates |= {v for _, v in edges - forest}
        nongates = {v for u, v in forest if len(incoming[v]) == 1}
        assert gates == set(range(n)) - nongates
        assert len(gates) >= expected
        best = min(best, len(gates))
    assert best == expected


def walk_original_forest_prefix(u, v, chains):
    assert u in chains[v]
    nodes = chains[v][chains[v].index(u):]
    return tuple(zip(nodes, nodes[1:]))


def solve_original_unit_distances(n, edges, source):
    outgoing = defaultdict(list)
    for u, v in edges:
        outgoing[u].append(v)
    labels, queue = {source: 0}, deque([source])
    while queue:
        u = queue.popleft()
        for v in outgoing[u]:
            if v not in labels:
                labels[v] = labels[u] + 1
                queue.append(v)
    return labels


def verify_cut_source_lifting(n, edges, forest, chains, h, roots, counts):
    for cut in enumerate_all_subset_choices(forest):
        live = edges - cut
        children = {v for _, v in cut}
        assert not (children & roots)
        assert all(not any(y == v for _, y in live) for v in children)
        counts['cut_cases'] += 1
        for source in range(n):
            entries = roots | children | {source}
            assert len(entries) <= len(roots) + len(cut) + 1
            owner = {v: next(x for x in reversed(chains[v]) if x in entries)
                     for v in range(n)}
            macros = defaultdict(list)
            for u, v in sorted(live):
                if (u, v) in forest and v not in entries:
                    continue
                g = owner[u]
                witness = walk_original_forest_prefix(g, u, chains) + ((u, v),)
                assert not (set(witness) & cut) and v in entries
                macros[g].append((v, 1 + h[u] - h[v], witness))
            labels, paths = {source: (0, 0)}, {source: ()}
            queue = [(0, 0, source)]
            while queue:
                cost, hops, u = heappop(queue)
                if labels[u] != (cost, hops):
                    continue
                for v, reduced, witness in macros[u]:
                    candidate = (cost + reduced, hops + len(witness))
                    if v not in labels or candidate < labels[v]:
                        labels[v], paths[v] = candidate, paths[u] + witness
                        heappush(queue, (*candidate, v))
            assert len(labels) <= len(roots) + 1
            assert not ((children - {source}) & labels.keys())
            raw = solve_original_unit_distances(n, live, source)
            for target in range(n):
                counts['target_checks'] += 1
                g = owner[target]
                if g not in labels:
                    assert target not in raw
                    counts['unreachable_targets'] += 1
                    continue
                suffix = walk_original_forest_prefix(g, target, chains)
                walk = paths[g] + suffix
                assert labels[g][0] + h[target] - h[source] == raw[target]
                assert labels[g][1] + len(suffix) == len(walk) == raw[target]
                vertex, seen = source, {source}
                for edge in walk:
                    u, v = edge
                    assert edge in live and u == vertex and v not in seen
                    vertex = v
                    seen.add(v)
                assert vertex == target
                counts['lifted_paths'] += 1
            counts['source_queries'] += 1


def run_bounded_unit_review():
    counts = Counter()
    for n in range(5):
        possible = {(u, v) for u in range(n) for v in range(n) if u != v}
        for edges in enumerate_all_subset_choices(possible):
            forest, chains, h, roots, q, incoming = build_unit_eligibility_forest(n, edges)
            verify_minimum_gate_count(n, edges, q, incoming, counts)
            verify_cut_source_lifting(n, edges, forest, chains, h, roots, counts)
            counts['exhaustive_graphs'] += 1
    edges = {(0, 1), (1, 0), (2, 3), (3, 2), (3, 4)}
    forest, chains, h, roots, q, incoming = build_unit_eligibility_forest(6, edges)
    assert q == 3
    verify_minimum_gate_count(6, edges, q, incoming, counts)
    verify_cut_source_lifting(6, edges, forest, chains, h, roots, counts)
    star = {(0, 1), (0, 2), (0, 3)}
    assert build_unit_eligibility_forest(4, star)[4] == 1
    assert build_unit_eligibility_forest(4, {(v, u) for u, v in star})[4] == 4
    assert 2 not in solve_original_unit_distances(3, {(0, 1), (0, 2)}, 1)
    parallel = {0: (0, 1), 1: (0, 1)}
    chains = derive_forest_ancestor_chains(2, {parallel[0]})
    assert parallel[1][0] in chains[parallel[1][1]]
    assert solve_original_unit_distances(2, {parallel[1]}, 0)[1] == 1
    assert walk_original_forest_prefix(0, 0, [(0,)]) == ()
    assert 1 + 0 - 0 > 0  # A unit loop is dominated by the empty path.
    assert counts['infeasible_forests'] and counts['unreachable_targets']
    print('PASS', dict(sorted(counts.items())))


if __name__ == '__main__':
    run_bounded_unit_review()
```

### Execution Record

The command above completed with exit status 0. Exact output:

```text
PASS {'admissible_forests': 77180, 'cut_cases': 12990, 'exhaustive_graphs': 4166, 'infeasible_forests': 33840, 'lifted_paths': 133883, 'source_queries': 51783, 'target_checks': 206681, 'unreachable_targets': 72798}
```

Forest/query counters include the extra six-vertex fixture; `exhaustive_graphs`
counts only the 4,166 graphs on 0-4 vertices. All 133,883 reachable results
passed original-edge membership, continuity, length, target and simplicity
checks; the other 72,798 results were unreachable in both representations.
The 33,840 rejected forests have infeasible zero-edge potential constraints;
cyclic selected-parent choices are discarded separately and not counted there.

Finite checks do not establish historical priority, implementation complexity,
physical resource use, or source-graph eligibility. The proofs above, not a
benchmark, justify the theorem. The lead's source-profile update was received
but neither reproduced nor used as independent theorem-check evidence; no
source measurements are imported here. A negative practical gate is compatible
with a correct exact eligibility theorem.

**Terminal result: GO for `D=r=0` and `q_min=n-n1+c1` within the specified
quotient; NO-GO for the stronger scope and novelty claims identified above.
Bounded review complete.**

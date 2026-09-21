# Connectivity And Paths: Independent Mathematical Review

Date: 2026-09-20. Scope: final A02/A03 research candidates only. This review owns only this file; author documents, production code, shared evidence/journals and A01 are unchanged. No commits.

## Findings First

**Verdict: the specified mechanisms survive independent mathematical review; their paper-level differentiation does not yet.** I found no incorrect accepted partition, distance, or parent witness. This is not a request to reopen production engineering. Four bounded findings follow. Priority refers to the user's research-differentiation objective, not to a deployed software incident.

### R1: A03's State Quotient Is Exactly Single-Predecessor Elimination

**High priority: novelty boundary, not a correctness failure.** Locations: [head closure](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Certified-Paths.md:84), [source promotion](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Certified-Paths.md:104), [surviving novelty claim](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Certified-Paths.md:297).

The document says an established contraction optimizer *might* reproduce the reduction. A stronger statement is possible: **ordinary directed vertex elimination reproduces its source-specific quotient exactly.** This is an independent reduction, not an assertion that CCH contains this particular ledger implementation.

1. Work in the reduced-cost retained graph R = F union X after domination pruning. Every nongate vertex has exactly one incoming arc: its zero-cost F parent. Every exception head is a gate; every F root is a gate.
2. Keep P union {s}. Eliminate any other vertex v with unique predecessor p. Replace each outgoing v->z by p->z with the same reduced weight, retaining the outgoing original edge ID. The incoming weight is zero. Drop nonnegative self-loops for this fixed source.
3. The remaining nongates still have one predecessor. Iteration redirects each exception/connector tail to its nearest retained ancestor, precisely pi_s. Internal F edges disappear. Result: A03's effective quotient, including the activated dormant arcs and the newly exposed connector into s.
4. There is no predecessor-times-successor fill explosion: each eliminated vertex has one predecessor, so each outgoing record is redirected at most once in that elimination step. The current working graph never grows. A naive elimination implementation may repeatedly redirect the same record along a long chain; computing nearest retained ancestors and mapping each record once gives the same terminal graph directly. Neither the terminal size nor its construction needs a generic all-pairs shortcut matrix.

This argument is more specific than analogy to VC-index or a citation to generic contraction. The independent probe checks equality of the complete non-self arc multiset, including original edge IDs, not just distances: 12,879 randomized source queries plus eight fixed-fixture queries passed.

[CCH, Section 2](https://ben-strasser.net/paper/customizable_contraction_hierarchies_arxiv_preprint.pdf) supplies the established distance-preserving elimination/shortcut framework, and Section 7.5 handles directions through asymmetric metrics. Its metric-independent topology and road-network assumptions are not A03's forest construction. Earlier [Bein, Kamburowski and Stallmann](https://epubs.siam.org/doi/10.1137/0221065) explicitly define unit-indegree node reduction; their two-terminal DAG optimization theorem is not a theorem about A03's arbitrary-source cyclic weighted contract. Those scope differences do not defeat the constructive equivalence above.

**Candidate correction:** present the quotient theorem as a specialization of single-predecessor elimination. Retain the tail-aware representation as a concrete *source-access index* candidate. A paper claim must distinguish that representation's read/storage behavior from equally optimized elimination, rather than claim q-state compression or no-fill contraction as new.

#### Promotion Is Optional; Tail Information Is Not

There is an exact alternative with **no virtual gate or owner split**. For an interior source s in base capsule g, define

```text
R_s = {v : pi(v)=g and s is an F ancestor of v}
alpha_s(p) = min { r(u,p) : (u,p) is a ledger record and u is in R_s }
Q = unchanged base-gate quotient, ignoring its self-arcs
Delta(p) = min_z [alpha_s(z) + dist_Q(z,p)]
rho_s(v) = 0 if v in R_s, else Delta(pi(v))
dist_G(s,v) = rho_s(v) + h(v) - h(s)
```

An empty minimum is infinity. For s already a gate, use ordinary Dijkstra from s. For interior s, initialize base-gate Dijkstra with alpha_s. **Dormant self-arcs participate in alpha_s**, even though Q ignores them.

Proof: before first reaching a base gate, a retained path from s can only follow F inside R_s. Its first exit is a ledger arc with a tail in R_s. Conversely each such tail is reachable from s at reduced cost zero. Thereafter Q exactly represents travel between base gates. Every vertex in R_s already has optimal reduced distance zero by nonnegativity. Taking minimum over the possible first exits proves the formula. Keep each seed's original tail/edge witness; gate predecessors then use settlement order and R_s interiors use parents back to s.

This costs q distance states and at most one source-access read of g's ledger row plus an ordinary expansion of that row if g becomes reachable. It has the same asymptotic two-read guarantee as the proposed split. The probe checked distances and complete parent chains for both formulations. Thus promotion survives as a correct convenient implementation, **not a necessary or distinguishing shortest-path principle**.

Tail provenance does survive the challenge. Independent four-vertex examples:

- Tree 0->1->2 with unit weights, separate root 3, h=(0,1,2,0), exceptions 0->3 and 2->3 both weight 1. Both map to base pair (0,3); their reduced costs are 1 and 3. Keeping only the cheaper pair arc loses source 2's only route to 3, whose actual distance is 1.
- Branching tree 0->1, 0->2, 1->3 with unit weights and return 3->0 of weight 0. From source 3 the distances are (0,1,1,0). The return is dormant at base gate 0; dropping it loses both ancestors and sibling branch 2. Both the promoted and seeded formulations recover the answer.

The requirement is to preserve enough tail-sensitive access information, not literally every original record. Same-tail/head parallel minima are safe, and a richer range-minimum index can encode equivalent information. The present ledger is sufficient, not proved space-minimal.

### R2: A02's O(d+a) State Mechanism Is Already Sensitivity-Oracle Structure

**High priority: novelty boundary, not a failed bound.** Locations: [fragment count](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Witness-Connectivity.md:185), [composition](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Witness-Connectivity.md:253), [novelty verdict](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Witness-Connectivity.md:341).

[Duan-Pettie, Theorem 2.1](https://arxiv.org/pdf/1607.06865) already distinguishes d failed forest edges from other failed edges and counts at most 2d fragments of affected trees. Corollary 2.2 uses 2d+1 Euler intervals, predecessor ownership, and an auxiliary connectivity graph. This is closer than merely saying both algorithms use Euler tours. A02's addition of at most two old components per insertion is elementary bookkeeping on that structure, not a new failure-parameter theorem.

Independent checks support the exact claimed relation:

```text
q = d + c_active <= min(n, 2d + 2a).
```

The count is tight: two disjoint failed tree edges plus two insertions joining four other singleton components activate eight fragments. A ninth untouched isolate stays implicit. With d=a=0, q=0 independently of the total number of old components or non-tree deletions. No c0-sized mutable map is necessary.

The **heterogeneous cover composition is sound**. Let B=F0 minus D, deleting missing forest-edge identities, and let S be the selected surviving cover. By the cut lemma each accepted local certificate preserves its own raw subgraph's partition. Connectivity-equivalent undirected subgraphs remain equivalent under union with any common graph. Therefore S union A union B has G's partition, and contraction of B is valid even when no selected certificate contains or aligns with F0. Certificate-local roots are irrelevant. This is exactly the kind of union closure formalized by [Improved Sparsification, Section 3, Definition 1](https://ics.uci.edu/~eppstein/pubs/EppGalIta-TR-93-20.pdf).

The author already disclaims these broad inventions; the consequence should be decisive, not softened into a new graph-algorithm claim. **What remains unresolved is the immutable heterogeneous-capacity read policy**, its membership/debit costs, and whether it improves a precisely matched storage/read problem. The finite laminar recurrence is correct and not new in itself. A tuned sensitivity-oracle baseline must also leave untouched components implicit and use the same canonical-output convention.

**Candidate correction:** keep A02 as a certificate-access/read-selection candidate. Do not sell implicit isolates, nested intervals, or fragment contraction as the novel theorem. A distinguishing theorem would need to concern the policy problem with explicitly equal preparation/storage allowances, rather than compare only against an n-entry DSU.

### R3: The Fragment Solver Can Safely Reject Fewer Certificates

**Medium priority: a concrete strengthening, not a bug in the conservative rule.** Locations: [witness debit](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Witness-Connectivity.md:65), [fragment procedure](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Witness-Connectivity.md:227).

Let B=F0 minus D and beta(v) be its fragment. For any retained H(x,k), define

```text
relative_debit(x,k) = number of deleted H(x,k) identities (u,v)
                      with beta(u) != beta(v).
relative_debit(x,k) < k
    => CC(B union (H(x,k) minus D))
       = CC(B union (Ex minus D)).
```

**Independent proof:** contract B in Ex and H, preserving parallel identities and dropping loops. A cut in the contracted graph is a union of B fragments, so the original residual-forest cut inequality still holds. Only deleted certificate identities that are non-loops after contraction can affect any such cut. Applying the strict witness-hit argument to this contracted certificate proves the equality. Equivalently, if a component of B union (H-D) has a surviving Ex edge crossing it, its cut crosses no B fragment; the usual cut contradiction counts only relative debit. A local substitution remains correct under any further union, so heterogeneous k and mixed-depth frontiers still compose in the **B-augmented** solver.

Strict improvement fixture: Ex is the triangle {01,02,12}, H(x,1)={01,02}. Globally F0={02,12,23}; delete {01,23}. Then the ordinary debit is 1 and rejects k=1, while relative debit is 0 because 0 and 1 remain in B fragment {0,1,2}. H-D alone does **not** preserve Ex-D, but B union (H-D) does. This distinguishes the stronger theorem from an invalid weakening of the standalone certificate lemma.

An even simpler specialization: **when d=0, skip all base-certificate reads and debit/frontier work after establishing that F0 is intact.** Union the old component IDs of live insertion endpoints and stream the existing labels through those unions. This remains exact with arbitrary non-tree deletions and with insertion-caused merges. It is not limited to the unchanged-answer case d=a=0. Establishing K and A still has its input/index cost.

The independent probe found 1,509 certificate checks accepted by relative debit but rejected by ordinary debit, with no incorrect B-augmented partition; it also tested complete frontiers for both rules.

**Limit:** this is another application of the same cut-certificate theorem, not a claim of new graph theory. Relative debit is query-fragment-dependent: it cannot be maintained by blindly reusing the immutable witness-hit counters. A tree cut or restoration can change the relevance of previously deleted witnesses even if their deletion status is unchanged. Price the endpoint remapping of deleted certificate occurrences, or retain the conservative rule when cheaper. The optional extension should never silently change the standalone solver's acceptance predicate.

### R4: One A03 Proof Sentence Needs Its Graph Qualified

**Low priority: local proof repair.** [Correctness Section 2](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Certified-Paths.md:172) says every source-to-v path first reaches its gate. This is true in the **retained graph after domination pruning**, not necessarily the original graph.

Counterexample to the unqualified sentence: F has 0->1->2 with weights 1,1; another root 3 has arc 3->1 of weight 1. Let h=(0,1,2,0) and add the tight dominated shortcut 0->2 of weight 2. Vertex 1 is a gate, so pi(2)=1. The original path 0->2 bypasses gate 1. Removing that shortcut leaves equal distances, and the retained path 0->1->2 does pass through the gate.

**Candidate correction:** begin the capsule lemma with "In the retained graph R=F union X, after source promotion ..." and transfer its distance identity back to G using the domination lemma. The existing sequence of lemmas already supports this repair; no algorithm change is required.

## Checks That Survived

These are positive mathematical findings, not additional requests:

- **Nested cuts:** half-open subtree intervals are laminar. Pop all intervals ending at a boundary before processing starts. The deepest surviving stack entry describes ownership; the same fragment may label several spans. Each active root supplies one span before cuts; each cut adds at most two boundaries. Hence at most 2d+c_active spans, without an n-entry owner array or an original-height stack.
- **Canonical minima:** final quotient unions must precede label reduction. Damaged old components contribute their actual external IDs; intact active components contribute their stored minima; inactive components pass through. No stale subtree minimum or Euler rank is substituted. The random probes use distinct, permuted external IDs and compare every output label and spanning-forest edge count.
- **Stable identities:** both probes preserve parallel identities. An alternative parallel tree arc does not revive a deleted chosen parent identity; it may reconnect through the cover or enter X. Loops contribute neither false connectivity nor a directed-parent cycle.
- **Witness cover:** `debit < k` is sufficient even with arbitrarily many deleted nonwitness edges. The strict inequality cannot generally become `<=`: two vertices with k+1 parallel edges, H containing k, and those k deleted leave a live raw connection absent from H. Restore/delete status must remain relative to the immutable epoch.
- **Paths:** feasible potentials plus acyclic tight parents suffice; the potential need not come from a particular anchor. The independent random generator deliberately uses arbitrary feasible potentials, zero weights, multiple roots, stable parallel arcs, and batched parent deletions/increases. Promotion excludes descendant base capsules. Gate predecessors on strict relaxations from settled vertices remain acyclic when lifted; the source-seeded alternative also passed parent-chain checks.

The new frontier probe addresses one bounded gap in [the author's evidence](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/connectivity-paths-evidence.md:102): its enumerator offers RAW only at leaves and one k per node. The published recurrence offers RAW at every node and multiple retained k options. The independent probe includes all those choices, compares against all 26 eight-leaf antichains, and selected internal RAW in 2,163 cases and heterogeneous certificate orders in 416. This is a coverage correction, not a counterexample to the DP proof.

## Primary-Source Boundary

Only primary papers or author slides support literature claims here. Scope was checked, not inferred from similar terminology.

| Source | Inspected material and relevance |
| --- | --- |
| [Nagamochi-Ibaraki, Algorithmica 1992](https://gi.cebitec.uni-bielefeld.de/_media/teaching/2013summer/936nagamochi-ibaraki-1992-sparse-k-connected-subgraph.pdf) | Indexed primary first-page text/abstract: sparse k-connectivity background. Full PDF fetch failed. No claim that arbitrary repeated forests are that paper's linear-time algorithm. The cut proof is checked directly here and in A02. |
| [Eppstein-Galil-Italiano, Improved Sparsification, 1993](https://ics.uci.edu/~eppstein/pubs/EppGalIta-TR-93-20.pdf) | Section 3, Definitions 1-2 and tree construction. Occupies strong-certificate composition and maintaining sparse certificates through a hierarchy; does not establish optimality of this specific immutable read policy. |
| [Duan-Pettie, 2017 version](https://arxiv.org/pdf/1607.06865) | Section 2, Theorem 2.1, Figure 1, Corollary 2.2 and its proof. Direct comparator for A02's state mechanism, not merely a vertex-failure analogy. Its retained range index and replacement-edge recovery differ from streaming a selected certificate cover. |
| [Patrascu-Thorup, author slides](https://people.csail.mit.edu/mip/papers/edgedel/talk.pdf) | Slides 4-8 and 15: prepared batch insert/delete connectivity, expander hierarchy, component oracle; approximation qualification is on slide 7. The [paper](https://people.csail.mit.edu/mip/papers/edgedel/paper.pdf) did not parse in this review, so no assertion of an independent full-paper audit. Broader batch baseline, not the closest interval mechanism. |
| [Cheng et al., VC-index, SIGMOD 2012](https://www.cse.cuhk.edu.hk/~jcheng/papers/VCindex_sigmod12.pdf) | Section 2 explicitly uses weighted **undirected**, positive-integer simple graphs. Algorithm 2 already inserts a source outside the retained set; Section 5.2/Theorem 3 supplies distance graphs, and the hierarchical index is the actual method. Source promotion and reconstruction broadly are occupied. A03's one-entry directed branching capsules are not literally vertex covers, and the published VC-index is not a drop-in directed zero-weight oracle. |
| [Dibbelt-Strasser-Wagner, CCH](https://ben-strasser.net/paper/customizable_contraction_hierarchies_arxiv_preprint.pdf) | Section 2 weighted elimination; Section 7.5 directed metrics. Establishes the contraction comparator; R1 supplies the exact specialization independently. Do not identify a generic CH's cumulative shortcut store with the terminal quotient size. |
| [Eppstein, Finding the k Shortest Paths](https://www.ics.uci.edu/~eppstein/pubs/Epp-SJC-98.pdf) | Section 2, Lemmas 1-3 and Hout/HG construction. Besides reduced-cost telescoping, it explicitly indexes sidetracks by their original tails along tree paths. A03's region is a source-descendant part of a capsule, not the fixed-target tree suffix used there; neither tails nor reusable access along a tree are new in the broad sense. |
| [Bilo et al., Fixed-Parameter Sensitivity Oracles](https://arxiv.org/html/2112.03059) | Theorem 15 proof and Section 4/Lemma 16. Covers a directed vertex-cover sensitivity construction with source/target access and an unweighted bounded-distance-increase preserver using slack. This is not A03's arbitrary weighted, arbitrary-source gate theorem, but rules out relabeling those bounded-detour/slack ideas as new. |
| [Bein-Kamburowski-Stallmann, Optimal Reduction of Two-Terminal DAGs](https://epubs.siam.org/doi/10.1137/0221065) | Publisher abstract only. Confirms historical unit-indegree node reduction; not used to transfer its DAG optimization theorem to weighted cyclic graphs. |

## Bounded Next Decision

**A02:** sound algorithmic composition, not a new connectivity theorem. A concrete strengthening is fragment-relative debit and its zero-tree-cut bypass. The remaining research question is whether a specified immutable certificate portfolio/read policy admits a meaningful separation or optimality result beyond the elementary laminar DP, against the same-state sensitivity/sparsification comparators. This review does not certify that such a result exists.

**A03:** sound specialized elimination with a tail-sensitive source-access representation. The useful contract is sharper than an unchanged-answer certificate, but its quotient and one-source repair reduce exactly to familiar elimination/access principles. A bounded next candidate is a capsule-local interval-minimum access index: alpha_s(p) is a minimum by head over original tail preorder positions in the source subtree, within row g. Test representation/read savings against the same operation on the elimination baseline. A range index is itself established machinery; calling it a new portal algorithm would not resolve novelty.

Do not accept either candidate as a genuinely differentiated algorithms-paper contribution solely because these correctness probes pass. Equally, do not discard their correct mechanisms because the broad novelty is occupied. The author files can remain closed; these are review findings and candidate directions for the lead, with no demand for a production implementation or an unrestricted new research program.

## Reproducible Results

Command used from the repository root:

```sh
awk '/^```python$/{p=1; next} p && /^```$/{p=0; next} p' research_algorithms_20260920/Connectivity-Paths-Independent-Review.md | python3 -
```

The random seed is fixed. WCC varies forest witnesses independently from hierarchy certificates, uses optional k=1/2/3 at every node, includes current insertions and parallel identities, and permutes canonical IDs. The path probe tests 1,500 prepared/current generations on nine vertices; 69 violate the old potential and are explicitly rejected, not counted as solved by the fast path. In every admitted case it compares original-arc Bellman-Ford with promotion, with generic elimination, and with source-seeded base Dijkstra. Both reconstruction modes are checked against original edge identities.

Observed complete output:

```text
wcc {'cases': 2000, 'frontiers': 4000, 'heterogeneous_selected': 416, 'internal_raw_selected': 2163, 'q_zero': 282, 'relative_accepts': 61216, 'strict_relative_accepts': 1509} mismatches=0
paths {'admitted_generations': 1431, 'elimination_equalities': 12879, 'interior_sources': 1996, 'parent_chains': 173106, 'rejected_generations': 69, 'seeded_equalities': 12879, 'source_queries': 12879} mismatches=0
pair_minimum_counterexample full_distance=1 tail_discarded_distance=inf
dormant_branch_counterexample full_distances=[0,1,1,0] discard_loses_vertex_2
fixed_path_checks {'admitted_generations': 2, 'elimination_equalities': 8, 'interior_sources': 5, 'parent_chains': 46, 'seeded_equalities': 8, 'source_queries': 8} mismatches=0
relative_debit_fixture ordinary=1 relative=0 standalone_wrong_augmented_exact
wcc_corners tight_q=8_with_one_implicit_isolate nested_holes_and_equal_ends=passed
strict_debit_boundary debit_equals_k_cannot_be_accepted
proof_scope_fixture raw_shortcut_bypasses_gate_1 retained_distances_equal
```

These counts are finite checks, not exhaustive coverage of nine-vertex graphs or proof by testing. The generic-elimination and source-access equalities also have the direct arguments in R1. The author's existing exhaustive results were read, not silently reclassified as independent results.

## Reviewed Versions

SHA-256 of the three inputs, recorded before edits and checked again at completion:

```text
6f5fb4f90a3f6c618016e577a5f7c3a47503f67ea548a1cdc5a488f9014e0bb1  Witness-Connectivity.md
ed35ea1b29790fb09e44bff604f1eaeaf80cb669e7ec6980bd82fbd6ddc2933f  Certified-Paths.md
927455f796b24599b101c2291a2c0a7b6972333015f471f2a369baf2e046aec6  connectivity-paths-evidence.md
```

## Independent Probe Source

These are independently written small-instance mathematical probes, not imports or replays of the author's solver. They intentionally use ordinary in-memory Python objects and do not establish physical RAM/I/O bounds. The WCC oracle is graph traversal; the path oracle is synchronous Bellman-Ford on original arcs. All finite arithmetic uses integers. `inf` is only an unreachable sentinel. Stable edge identities distinguish parallel arcs and tree witnesses.

Run the following block through Python stdin, or extract this sole `python` fence and execute it. It reads/writes no files.

```python
from bisect import bisect_right
from collections import Counter
from heapq import heappop, heappush
from itertools import combinations
from math import inf
from random import Random


def label_undirected_components_exact(n, edges):
    adj = [[] for _ in range(n)]
    for u, v in edges:
        adj[u].append(v)
        adj[v].append(u)
    labels = [-1] * n
    for root in range(n):
        if labels[root] >= 0:
            continue
        todo = [root]
        labels[root] = root
        while todo:
            u = todo.pop()
            for v in adj[u]:
                if labels[v] < 0:
                    labels[v] = root
                    todo.append(v)
    return labels


def select_residual_forests_exact(n, edges, identities, k):
    remaining = list(identities)
    selected = set()
    for _ in range(k):
        labels = list(range(n))
        taken = set()
        for identity in remaining:
            u, v = edges[identity]
            a, b = labels[u], labels[v]
            if a != b:
                labels = [a if c == b else c for c in labels]
                taken.add(identity)
        selected.update(taken)
        remaining = [e for e in remaining if e not in taken]
    return selected


def derive_rooted_coordinates_exact(parent):
    n = len(parent)
    children = [[] for _ in parent]
    for v, u in enumerate(parent):
        if u >= 0:
            children[u].append(v)
    tin, tout, roots, order = [0]*n, [0]*n, [0]*n, []

    def visit_rooted_vertex_exact(v, root):
        roots[v] = root
        tin[v] = len(order)
        order.append(v)
        for child in children[v]:
            visit_rooted_vertex_exact(child, root)
        tout[v] = len(order)

    for v, u in enumerate(parent):
        if u < 0:
            visit_rooted_vertex_exact(v, v)
    return tin, tout, roots, order


def root_undirected_forest_exact(n, edges, forest):
    adj = [[] for _ in range(n)]
    for e in forest:
        u, v = edges[e]
        adj[u].append((v, e))
        adj[v].append((u, e))
    parent, parent_edge = [-1]*n, [None]*n
    seen = set()
    for root in range(n):
        if root in seen:
            continue
        seen.add(root)
        todo = [root]
        while todo:
            u = todo.pop()
            for v, e in adj[u]:
                if v not in seen:
                    seen.add(v)
                    parent[v], parent_edge[v] = u, e
                    todo.append(v)
    return parent, parent_edge


def compare_fragment_cover_exact(rng, n, edges, old, deleted, added, counts):
    shuffled = sorted(old)
    rng.shuffle(shuffled)
    forest = select_residual_forests_exact(n, edges, shuffled, 1)
    parent, parent_edge = root_undirected_forest_exact(n, edges, forest)
    tin, tout, roots, order = derive_rooted_coordinates_exact(parent)
    cuts = {v for v, e in enumerate(parent_edge) if e in deleted}
    damaged = {roots[v] for v in cuts}
    active = damaged | {roots[v] for e in added for v in edges[e]}
    q = len(cuts) + len(active)
    assert q <= min(n, 2*len(cuts) + 2*len(added))
    counts['cases'] += 1
    counts['q_zero'] += q == 0
    spans = []
    for root in sorted(active, key=tin.__getitem__):
        local = [v for v in cuts if roots[v] == root]
        starts = {tin[v]: v for v in local}
        boundaries = sorted({tin[root], tout[root]} |
                            {x for v in local for x in (tin[v], tout[v])})
        stack = []
        for lo, hi in zip(boundaries, boundaries[1:]):
            while stack and tout[stack[-1]] <= lo:
                stack.pop()
            if lo in starts:
                stack.append(starts[lo])
            spans.append((lo, hi, stack[-1] if stack else root))
    assert len(spans) <= 2*len(cuts) + len(active)
    lefts = [lo for lo, _, _ in spans]

    def locate_active_fragment_exact(v):
        i = bisect_right(lefts, tin[v]) - 1
        return spans[i][2] if i >= 0 and tin[v] < spans[i][1] else None

    fragment = label_undirected_components_exact(n, [edges[e] for e in forest-deleted])
    owners = [locate_active_fragment_exact(v) for v in range(n)]
    for u in range(n):
        assert (owners[u] is not None) == (roots[u] in active)
        for v in range(n):
            if owners[u] is not None and owners[v] is not None:
                assert (owners[u] == owners[v]) == (fragment[u] == fragment[v])
    external = rng.sample(range(1, 100000), n)
    truth = label_undirected_components_exact(n, [edges[e] for e in (old-deleted)|added])
    expected = [min(external[u] for u in range(n) if truth[u] == truth[v])
                for v in range(n)]
    nodes, children = {}, {}

    def build_complete_frontier_exact(lo, hi):
        node = (lo, hi)
        raw = {e for e in old if lo <= e % 8 < hi}
        options = [(rng.randrange(1, 40), raw-deleted, 'raw')]
        relaxed = list(options)
        for k in (1, 2, 3):
            if rng.randrange(4) == 0:
                continue
            identities = sorted(raw)
            rng.shuffle(identities)
            cert = select_residual_forests_exact(n, edges, identities, k)
            cost = rng.randrange(1, 40)
            hits = len(cert & deleted)
            crossing = sum(fragment[edges[e][0]] != fragment[edges[e][1]]
                           for e in cert & deleted)
            option = (cost, cert-deleted, k)
            if hits < k:
                options.append(option)
            if crossing < k:
                relaxed.append(option)
                assert label_undirected_components_exact(n, [edges[e] for e in (forest|cert)-deleted]) == \
                       label_undirected_components_exact(n, [edges[e] for e in (forest|raw)-deleted])
                counts['relative_accepts'] += 1
                counts['strict_relative_accepts'] += hits >= k
        nodes[node] = (options, relaxed)
        if hi-lo == 1:
            return [(node,)]
        mid = (lo+hi)//2
        children[node] = ((lo, mid), (mid, hi))
        lhs = build_complete_frontier_exact(lo, mid)
        rhs = build_complete_frontier_exact(mid, hi)
        return [(node,)] + [a+b for a in lhs for b in rhs]

    covers = build_complete_frontier_exact(0, 8)
    assert len(covers) == 26
    for mode in (0, 1):
        choices = {node: min(options[mode], key=lambda x: x[0])
                   for node, options in nodes.items()}

        def solve_frontier_cost_exact(node):
            cost, payload, kind = choices[node]
            answer = (cost, payload, [(node, kind)])
            if node in children:
                lcost, ledges, lplan = solve_frontier_cost_exact(children[node][0])
                rcost, redges, rplan = solve_frontier_cost_exact(children[node][1])
                if lcost+rcost < cost:
                    answer = (lcost+rcost, ledges|redges, lplan+rplan)
            return answer

        cost, selected, plan = solve_frontier_cost_exact((0, 8))
        assert cost == min(sum(choices[node][0] for node in cover) for cover in covers)
        counts['frontiers'] += 1
        counts['internal_raw_selected'] += any(hi-lo > 1 and kind == 'raw'
                                               for (lo, hi), kind in plan)
        counts['heterogeneous_selected'] += len({kind for _, kind in plan if kind != 'raw'}) > 1
        uf = {v: v for v in cuts|active}

        def find_active_representative_exact(v):
            while uf[v] != v:
                v = uf[v]
            return v

        successful = set()
        for e in sorted(selected|added):
            u, v = edges[e]
            p, t = owners[u], owners[v]
            if p is None and t is None:
                assert roots[u] == roots[v]
                continue
            assert p is not None and t is not None
            p, t = find_active_representative_exact(p), find_active_representative_exact(t)
            if p != t:
                uf[p] = t
                successful.add(e)
        minima = {find_active_representative_exact(v): inf for v in uf}
        for root in active-damaged:
            f = find_active_representative_exact(root)
            minima[f] = min(minima[f], min(external[v] for v in range(n) if roots[v] == root))
        for v in order:
            if roots[v] in damaged:
                f = find_active_representative_exact(owners[v])
                minima[f] = min(minima[f], external[v])
        answer = [minima[find_active_representative_exact(owners[v])] if owners[v] is not None
                  else min(external[u] for u in range(n) if roots[u] == roots[v])
                  for v in range(n)]
        assert answer == expected
        witness = (forest-deleted)|successful
        assert label_undirected_components_exact(n, [edges[e] for e in witness]) == truth
        assert len(witness) == n-len(set(truth))


def compute_bellman_distances_exact(vertices, arcs, seeds):
    distances = {v: seeds.get(v, inf) for v in vertices}
    for _ in range(max(0, len(vertices)-1)):
        old = dict(distances)
        for u, v, weight, _ in arcs:
            distances[v] = min(distances[v], old[u]+weight)
    return distances


def run_seeded_dijkstra_exact(vertices, arcs, seeds):
    distances = {v: inf for v in vertices}
    entry, done, queue = {}, set(), []
    rows = {v: [] for v in vertices}
    for u, v, weight, edge in arcs:
        rows[u].append((v, weight, edge))
    for v, weight, edge in seeds:
        if weight < distances[v]:
            distances[v], entry[v] = weight, edge
            heappush(queue, (weight, v))
    while queue:
        distance, u = heappop(queue)
        if u in done:
            continue
        done.add(u)
        for v, weight, edge in rows[u]:
            if v not in done and distance+weight < distances[v]:
                distances[v], entry[v] = distance+weight, edge
                heappush(queue, (distances[v], v))
    return distances, entry


def compare_source_capsules_exact(parent, tree_ids, h, current, counts):
    n = len(parent)
    if any(weight+h[u]-h[v] < 0 for u, v, weight in current.values()):
        counts['rejected_generations'] += 1
        return
    counts['admitted_generations'] += 1
    f = [u if edge in current and current[edge][2]+h[u] == h[v] else -1
         for v, (u, edge) in enumerate(zip(parent, tree_ids))]

    def check_forest_ancestry_exact(u, v):
        while v >= 0:
            if u == v:
                return True
            v = f[v]
        return False

    exceptions = {e for e, (u, v, _) in current.items() if not check_forest_ancestry_exact(u, v)}
    gates = {v for v in range(n) if f[v] < 0} | {current[e][1] for e in exceptions}
    owner = []
    for v in range(n):
        while v not in gates:
            v = f[v]
        owner.append(v)
    kept_tree = {tree_ids[v] for v in range(n) if f[v] >= 0}
    retained = [(u, v, w+h[u]-h[v], e) for e, (u, v, w) in current.items()
                if e in exceptions|kept_tree]
    ledger = [arc for arc in retained if arc[3] in exceptions or owner[arc[0]] != owner[arc[1]]]
    base = [(owner[u], v, r, e) for u, v, r, e in ledger if owner[u] != v]
    raw = [(u, v, w, e) for e, (u, v, w) in current.items()]
    for source in range(n):
        counts['source_queries'] += 1
        g = owner[source]
        region = {v for v in range(n) if owner[v] == g and check_forest_ancestry_exact(source, v)}
        effective = [source if v in region else owner[v] for v in range(n)]
        promoted = [(effective[u], effective[v], r, e) for u, v, r, e in ledger
                    if effective[u] != effective[v]]
        if source not in gates:
            promoted.append((g, source, 0, tree_ids[source]))
            counts['interior_sources'] += 1
        pd, pe = run_seeded_dijkstra_exact(gates|{source}, promoted, [(source, 0, None)])
        truth = compute_bellman_distances_exact(list(range(n)), raw, {source: 0})
        original = [pd[effective[v]]+h[v]-h[source] for v in range(n)]
        assert original == [truth[v] for v in range(n)]

        # Ordinary vertex elimination, keeping exactly the gates and query source.
        contracted = list(retained)
        for vertex in range(n-1, -1, -1):
            if vertex in gates or vertex == source:
                continue
            incoming = [arc for arc in contracted if arc[1] == vertex]
            assert len(incoming) == 1 and incoming[0][2] == 0
            p, _, weight, _ = incoming[0]
            outgoing = [arc for arc in contracted if arc[0] == vertex]
            contracted = [arc for arc in contracted if vertex not in arc[:2]]
            contracted += [(p, v, weight+r, e) for _, v, r, e in outgoing if p != v]
        contracted = [arc for arc in contracted if arc[0] != arc[1]]
        assert sorted(contracted) == sorted(promoted)
        counts['elimination_equalities'] += 1

        # Unchanged base quotient with source-access seeds, no virtual owner state.
        seeds = [(source, 0, None)] if source in gates else \
                [(v, r, e) for u, v, r, e in ledger if u in region]
        sd, se = run_seeded_dijkstra_exact(gates, base, seeds)
        seeded = [(0 if v in region else sd[owner[v]])+h[v]-h[source] for v in range(n)]
        assert seeded == original
        counts['seeded_equalities'] += 1
        for mode in (0, 1):
            for target in range(n):
                if truth[target] == inf:
                    continue
                v, length, seen = target, 0, set()
                while v != source:
                    assert v not in seen
                    seen.add(v)
                    if mode == 0:
                        edge = pe[v] if v in gates|{source} else tree_ids[v]
                    else:
                        edge = tree_ids[v] if v in region or v not in gates else se[v]
                    u, head, w = current[edge]
                    assert head == v
                    length += w
                    v = u
                assert length == truth[target]
                counts['parent_chains'] += 1


rng = Random(90720260920)
wcc = Counter()
for trial in range(2000):
    n = rng.randrange(2, 11)
    core = rng.randrange(1, n+1)
    pairs = [(u, v) for u, v in combinations(range(core), 2) if rng.randrange(2)]
    pairs += [pair for pair in list(pairs) if rng.randrange(4) == 0]
    pairs += [(rng.randrange(core),)*2]
    edges = dict(enumerate(pairs))
    old = set(edges)
    deleted = {e for e in old if rng.randrange(3) == 0}
    added = set()
    for _ in range(rng.randrange(4)):
        e = len(edges)
        edges[e] = (rng.randrange(n), rng.randrange(n))
        added.add(e)
    compare_fragment_cover_exact(rng, n, edges, old, deleted, added, wcc)
print('wcc', dict(sorted(wcc.items())), 'mismatches=0')

paths = Counter()
for trial in range(1500):
    n = 9
    parent, tree_ids, h, current = [], [], [], {}
    for v in range(n):
        u = rng.randrange(v) if v and rng.randrange(5) else -1
        parent.append(u)
        tree_ids.append(v if u >= 0 else None)
        h.append(h[u]+rng.randrange(4) if u >= 0 else rng.randrange(5))
        if u >= 0:
            current[v] = (u, v, h[v]-h[u])
    for u in range(n):
        for v in range(n):
            if rng.randrange(5) == 0:
                current[n+len(current)] = (u, v, max(0, h[v]-h[u])+rng.randrange(5))
    for e in list(current):
        u, v, w = current[e]
        if rng.randrange(8) == 0:
            del current[e]
        elif rng.randrange(7) == 0:
            current[e] = (u, v, w+1+rng.randrange(3))
    if trial % 5 == 0:
        u, v = rng.randrange(n), rng.randrange(n)
        current[max(current, default=n)+1] = (u, v, rng.randrange(4))
    compare_source_capsules_exact(parent, tree_ids, h, current, paths)
print('paths', dict(sorted(paths.items())), 'mismatches=0')

# Pair aggregation loses the only source-accessible tail, although the cheaper
# base-gate shortcut is valid for base-gate queries.
fixed = Counter()
parent, tree_ids, h = [-1, 0, 1, -1], [None, 0, 1, None], [0, 1, 2, 0]
arcs = {0: (0, 1, 1), 1: (1, 2, 1), 2: (0, 3, 1), 3: (2, 3, 1)}
compare_source_capsules_exact(parent, tree_ids, h, arcs, fixed)
raw = [(u, v, w, e) for e, (u, v, w) in arcs.items()]
assert compute_bellman_distances_exact(list(range(4)), raw, {2: 0})[3] == 1
assert compute_bellman_distances_exact(list(range(4)), [a for a in raw if a[3] != 3], {2: 0})[3] == inf
print('pair_minimum_counterexample', 'full_distance=1', 'tail_discarded_distance=inf')

# A branch as well as ancestors must become reachable via the dormant return.
parent, tree_ids, h = [-1, 0, 0, 1], [None, 0, 1, 2], [0, 1, 1, 2]
arcs = {0: (0, 1, 1), 1: (0, 2, 1), 2: (1, 3, 1), 3: (3, 0, 0)}
compare_source_capsules_exact(parent, tree_ids, h, arcs, fixed)
raw = [(u, v, w, e) for e, (u, v, w) in arcs.items()]
assert list(compute_bellman_distances_exact(list(range(4)), raw, {3: 0}).values()) == [0, 1, 1, 0]
assert compute_bellman_distances_exact(list(range(4)), raw[:-1], {3: 0})[2] == inf
print('dormant_branch_counterexample', 'full_distances=[0,1,1,0]', 'discard_loses_vertex_2')
print('fixed_path_checks', dict(sorted(fixed.items())), 'mismatches=0')

edges = {0: (0, 1), 1: (0, 2), 2: (1, 2), 3: (2, 3)}
raw, cert, forest, deleted = {0, 1, 2}, {0, 1}, {1, 2, 3}, {0, 3}
fragments = label_undirected_components_exact(4, [edges[e] for e in forest-deleted])
hits = len(cert & deleted)
relative = sum(fragments[edges[e][0]] != fragments[edges[e][1]] for e in cert & deleted)
assert hits == 1 and relative == 0
assert label_undirected_components_exact(4, [edges[e] for e in cert-deleted]) != \
       label_undirected_components_exact(4, [edges[e] for e in raw-deleted])
assert label_undirected_components_exact(4, [edges[e] for e in (forest|cert)-deleted]) == \
       label_undirected_components_exact(4, [edges[e] for e in (forest|raw)-deleted])
print('relative_debit_fixture', 'ordinary=1', 'relative=0', 'standalone_wrong_augmented_exact')

corner = Counter()
local = Random(901)
edges = {0: (0, 1), 1: (2, 3), 2: (4, 5), 3: (6, 7)}
compare_fragment_cover_exact(local, 9, edges, {0, 1}, {0, 1}, {2, 3}, corner)
parent, parent_edge = root_undirected_forest_exact(9, edges, {0, 1})
tin, tout, roots, order = derive_rooted_coordinates_exact(parent)
cuts = {v for v, e in enumerate(parent_edge) if e in {0, 1}}
active = {roots[v] for v in cuts} | {roots[v] for e in {2, 3} for v in edges[e]}
assert len(cuts)+len(active) == 2*len(cuts)+2*2 == 8 and roots[8] not in active

edges = dict(enumerate([(0, 1), (1, 2), (1, 3), (0, 4), (4, 5), (4, 6)]))
for deleted in ({0, 1}, {0, 2}):
    compare_fragment_cover_exact(local, 7, edges, set(edges), deleted, set(), corner)
external = [70, 50, 1, 40, 20, 60, 30]
labels = label_undirected_components_exact(7, [edges[e] for e in set(edges)-{0, 1}])
assert min(external[v] for v in (1, 2, 3)) == 1
assert min(external[v] for v in range(7) if labels[v] == labels[1]) == 40
print('wcc_corners', 'tight_q=8_with_one_implicit_isolate', 'nested_holes_and_equal_ends=passed')

parallel = {0: (0, 1), 1: (0, 1), 2: (0, 1)}
cert = select_residual_forests_exact(2, parallel, [0, 1, 2], 2)
assert len(cert) == 2
assert label_undirected_components_exact(2, []) != \
       label_undirected_components_exact(2, [parallel[e] for e in set(parallel)-cert])
print('strict_debit_boundary', 'debit_equals_k_cannot_be_accepted')

# Original tight shortcuts can bypass a promoted head, despite an equivalent
# retained path passing through it. This probes the scope of the proof sentence.
arcs = [(0, 1, 1, 0), (1, 2, 1, 1), (3, 1, 1, 2), (0, 2, 2, 3)]
assert compute_bellman_distances_exact(list(range(4)), arcs, {0: 0}) == \
       compute_bellman_distances_exact(list(range(4)), arcs[:-1], {0: 0})
assert arcs[-1][:2] == (0, 2) and 1 not in arcs[-1][:2]
print('proof_scope_fixture', 'raw_shortcut_bypasses_gate_1', 'retained_distances_equal')
```

## Follow-On: Shared-Route Access Certificates

Date: 2026-09-20. Independent review of [Paths-Access-Frontier.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Paths-Access-Frontier.md). Append only. The earlier review and its probes are unchanged and were **not rerun**. The author's 4,800-query/1,440-update/12,600-drift-check results and the lead's replay are reported evidence, not counted as this review's independent tests.

**Verdict:** no counterexample to the new certificate under its stated snapshot, containment and exact-core premises. Its signed/failure drift rule is sound. Two conservative choices can be strengthened without changing the proof structure. The advertised family separates the method from scanners and mandatory per-head seed reporters, but a concrete factored transit-access representation matches that family, including its shared-metric update experiment. General equivalence to a published compressed TNR/CH implementation remains unresolved.

### AF1: Closure Substitution Survives The Circularity Attack

Locations: [tuple and guard](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Paths-Access-Frontier.md:61), [closure theorem](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Paths-Access-Frontier.md:92), [update rules](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Paths-Access-Frontier.md:149).

For a live original route to p, it is enough that `dist_Q(z,p) <= L1(p)`. The route need not be shortest. Even if a selected original witness ceases to be the minimum of its Q pair, the current exact pair minimum is no more expensive, so the selected original route still supplies an upper bound. Thus

```text
a + dist_Q(z,p) <= a + L1(p) <= L0(p) + b <= cost(e).
```

Together with the fact that the leader is itself a current input access, this proves both sides of the closure equality. The leader may reach an omitted access's head using core arcs contributed by the very same ledger. That is **not circular**: those arcs remain available in independently exact Q. It would become invalid if source-seed compression also deleted those core arcs. Q pair minima must be formed from all current ledger records, not only the query's selected seed records.

The independent probe uses identities 101 and 102 both as source-row records and selected Z witnesses. Reweighting, deleting, restoring or moving their heads updates both the corresponding header and the route state. It compares Q formed by exact pair minima with the full original core multigraph, and checks every emitted seed's current cost, identity and containment in the queried range. These are precisely the interactions absent from a probe whose route-edge array and indexed access row are disjoint.

Four small counterexamples clarify the necessary premises, **not defects in the published procedure**:

- **Partial range:** Q has z->p of cost 1. A parent cell contains `(z,0)` followed by `(p,2)` and passes its guard. Querying only the second record must return distance 2 to p and infinity to z. Reusing the parent's outside-range leader invents distances 1 and 0. The full-containment test prevents this.
- **Pruning Q as well:** Q has z->p of cost 0 and the source cell contains `(z,0),(p,0)`. Compressing the cell to z is valid. Also deleting Q's z->p arc because the p access was omitted makes p unreachable. Keep seed selection and core construction separate.
- **Last batch only:** old route z->r->p has costs 1,1. Increase the first edge to 4, then the second to 2. With accesses `(z,0),(p,4)`, b=2. Last-batch drift 1 passes, but cumulative drift 4 correctly rejects; unguarded substitution returns 6 instead of 4.
- **Stale selected leader:** with Q's z->p cost 1, reweight the leader `(z,0)` to `(z,7)` while retaining `(p,2)`. Using the old leader payload invents a zero-cost seed. Current route drift alone does not repair stale `a/leader`; its access header must also change.

Nested failures need counts, not a toggled boolean. Restoring one failed ancestor must not clear a still-failed descendant. The probe verifies that case and then a signed ancestor/descendant cancellation of -1. A surviving identity with changed endpoints is invalid for the old selected route; all 625 enumerated snapshots respect that distinction.

### AF2: Two Proven Tightenings

These are bounded mathematical extensions, not changes to the author file or claims of fresh graph-theoretic novelty.

**A. Keep the maximum signed drift, without flooring it at zero.** For nonempty H, use

```text
beta_signed(H) = max_{p in H} (L1(p)-L0(p)), with failures mapped to infinity.
beta_signed(empty) = 0.
accept when the existing finite/coverage tests hold and a+beta_signed(H) <= b.
```

The proof only needs `L1(p) <= L0(p)+beta`; a nonnegative beta is unnecessary. This strictly strengthens the guard, never weakens soundness. Example: a=5, one other access costs 5, old route length 3, current route length 0. Then b=2 and beta_signed=-3: the signed test accepts an exact replacement; flooring beta at zero rejects. This creates no negative Dijkstra edge. It changes only a comparison of certificate values. Empty/all-z cells retain their separate handling. In the independent range suite, this admitted larger cells in **790 ranges** while preserving closure.

**B. Reverify a fixed tree of gate pairs instead of pinning its initial original identities forever.** Preserve the directed topology of Z. For each tree pair `(parent_Z(v),v)`, bind its current exact Q minimum and current original witness. The pair is failed only when no current arc realizes it. Let `L_pair(p)` be the resulting tree-route length, retaining the original L0 for the access tuples.

Every such route is real while F is unchanged; its current original witness supplies the required lift. Moreover `L_pair(p) <= L1(p)` whenever the original selected route survives, and a live alternative may keep L_pair finite when the old identity fails. The same cumulative subtree-event construction and guard therefore work with L_pair. This formalizes the author's permitted "explicitly reverified replacement" without running a new SSSP.

Example: old selected z->p has cost 1; an alternative has cost 2. Delete the selected identity. With accesses `(z,0),(p,3)`, the published selected-identity rule rejects because its route failed. Rebinding the verified pair to the cost-2 original arc gives drift 1 and b=2, so substitution is exact. The probe's pair-bound variant compressed more aggressively in **2,624 ranges**.

The obligation changes precisely: compute pair minima independently from complete current records first; update the current witness ID even for an equal-weight replacement; debit drift by cumulative **tree-pair weight/absence changes**, not by the old selected-ID counter. An endpoint move removes an arc from its old pair and may add it to another. Never treat an identity match or an unverified alternative as proof. The new parameter is the number of changed tree pairs; it need not equal the manuscript's t. This is a fixed-topology metric/witness update, not permission to alter Q circularly.

### AF3: The Tuple Cannot Be An Exact Domination Test

There is an unavoidable loss beyond hull holes: b forgets which head attained each access slack. Let Z be a star with old lengths `L0(p1)=L0(p2)=1`; current lengths are 2 and 1. Consider these two alternative source cells, with the same leader identity/tail and the same nonleader head/tail identities:

```text
C_good = {(z,0), (p1,2), (p2,1)}
C_bad  = {(z,0), (p1,1), (p2,2)}.
```

Both have `a=0, b=0`, the same head hull, zero bad count, and count 3. The shared envelope is identical. C_good can be replaced exactly by its leader; C_bad cannot, because its direct p1 access costs 1 instead of the route's 2. The guard rejects both because beta=1.

Even **Q can be identical** in the two instances. Use another source capsule g with unchanged, outside-query tail records realizing Q arcs g->z, g->p1 and g->p2 of cost 0. Add only z->p1 of cost 2 and z->p2 of cost 1 as other core arcs. The lower outside-query records fix the same Q pair minima in both instances, but g is unreachable after entering z, so they do not invalidate the distinction between the two source ranges. The retained probe checks this exact common core.

Therefore no predicate using only this tuple and the route envelope can always decide exact leader domination. A stronger complete test needs head/slack correlation or additional work. The author's guard is correctly sufficient-only; the example is a limitation of the compressed information, not a false acceptance. It also shows why substituting the source capsule's Q row minima for its restricted access range would be wrong.

### AF4: A Factored Transit Comparator Matches The Changed-Answer Family

[Transit Node Routing Reconsidered, Section 4, post-search stalling](https://arxiv.org/pdf/1302.5611) already removes a candidate access when another access plus a transit route is no more expensive. A02/A03 terminology is unnecessary to interpret the new guard: it is a batched sufficient test of those inequalities using a shared route upper bound. The same paper discusses access-set overlap compression; that is not evidence that it contains this specific two-minimum/dynamic-envelope index. [CCH, Section 7.7](https://ben-strasser.net/paper/customizable_contraction_hierarchies_arxiv_preprint.pdf) propagates affected metric changes rather than requiring unconditional rebuilding; it does not establish the new t-parameter bound. General published-implementation equivalence remains unproved.

A more concrete comparison is available for [the stated family](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Paths-Access-Frontier.md:175), including [its update experiment](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Paths-Access-Frontier.md:191). Write `A_i=C*i`, with `C>k+2`. On the explicit input, a preprocessing scan can verify and retain:

- One per-block offset A_i and the b/x tail type; the b_i access label is the real record `(z,A_i)`.
- One shared head-cost profile `j+2`, rather than k materialized distances for every x_i.
- The current star metric `w_j`, plus one shared validity condition `max_j(w_j-j) <= 2`. Require all w_j finite and nonnegative.

This costs O(K+k) scalar/provenance entries for the family, in addition to the same retained raw input. No formula is supplied as privileged input: the scan must establish it, and a mismatch needs ordinary fallback. For each b_i, the one stored transit access is exact while the shared condition holds, and the result at p_j is `A_i+w_j`. The original leader costs and IDs do not change when w_j changes. For x_i, the exact p_j result remains `A_i+j+2`: any later access to z costs at least `A_i+C`, already larger than all those direct costs. Its z distance is `A_(i+1)`, or infinity for the last x. Thus the last x's required direct access profile is preserved, not globally deleted.

The probe verifies all b_i and x_i for K=32, k=31 under four metrics: baseline; the author's single +1 edit; +2 on every star edge; and all star weights decreased to zero. **256 source checks pass**, with one transit access per b query and no per-source access-label updates. The shared metric/validity state changes; core/output work still counts. This construction uses 32 b-access records per metric, versus the author's 81 frontier records, which already reduce to the same 32 distinct seeds.

Consequently, neither 81-versus-1,024 nor changed answers without rewriting the large source row separates this candidate from a factored transit-access baseline. It does separate it from a procedure required to enumerate all per-head minima. The construction does **not** prove equivalence for arbitrary ledger edits, arbitrary source trees or all graphs; those are the unresolved parts. The surviving candidate is a particular dynamic range-validity representation, not a new min-plus access-pruning principle.

### Independent Evidence And Scope

This new probe checks only the new source-access/core-closure interface and route certificate. It does not run the old elimination probe, an original full-graph SSSP implementation, or a storage engine. Exact core distances are a tiny Floyd-Warshall oracle on a six-gate multigraph. The five-vertex route tree has nontrivial preorder and nested cuts. All 5^4=625 route-edge states are enumerated: baseline, zero weight, increased weight, absent, and a live identity moved to a different head. Access-header updates occur in the same snapshots.

It checks all 36 nonempty intervals of an eight-slot tail row per snapshot, including deleted slots, leaders outside partial ranges, outside-Z heads, dormant returns, equal weights, and shared route/access identities. These are 22,500 abstract source ranges, not a claim that all intervals are subtrees of one fixed original F. Testing all intervals is stronger than the required contiguous-range closure property. Route events persist across snapshots and are adjusted to cumulative current differences; direct root-path sums independently check the envelope. The pair-minimum variant is an independently verified route-bound extension, not an implemented replacement for the author's update engine.

Run **only the appended probe**, from the repository root:

```sh
awk '/^```python3$/{p=1; next} p && /^```$/{p=0; next} p' research_algorithms_20260920/Connectivity-Paths-Independent-Review.md | python3 -
```

Observed stdout, exit status 0:

```text
independent_access {'accepted_cells_0': 20629, 'accepted_cells_1': 20549, 'accepted_cells_2': 20366, 'closure_checks': 67500, 'drift_hulls': 9375, 'leaf_updates': 3050, 'pair_strict_ranges': 2624, 'shared_route_leaf_updates': 30, 'signed_strict_ranges': 790, 'snapshots': 625, 'source_ranges': 22500} mismatches=0
mutation_checks partial_leader/core_pruning/last_batch/stale_leader=detected
strict_extensions signed_decrease=passed reverified_parallel_route=passed
factored_family {'single_transit_accesses': 128, 'source_checks': 256} mismatches=0
summary_indistinguishability same_a_b_hull_core only_one_cell_is_dominated
nested_restoration child_failure_preserved signed_cancellation=-1
```

Modes 0/1/2 are the published clipped selected-route bound, signed selected-route bound, and clipped reverified-pair bound. Counts include 3,050 leaf writes, of which six initialize live slots; the 30 shared-route leaf writes include two initializations. Accepted-cell counts can fall for a stronger method because one accepted ancestor replaces several accepted descendants. The 67,500 closure checks are three methods on the same 22,500 ranges, not that many independent input ranges. Explicit bad-premise fixtures demonstrate counterexamples; they are not claims that the author's guarded procedure failed.

Provenance: the author file SHA-256 is `2c3002fe605f52304d4ff356273d370eb7d1ff3fdc8adff93ecda671f29b8845`. The original review prefix is exactly 44,865 bytes / 639 lines, SHA-256 `670a951701662e1b0f066882acf45d16eca2ee5b21c3fdf5813b72877b070f33`. Verify that prefix and author hash after this append. No author file, production file or shared journal was edited; no commits.

### Retained Follow-On Probe

<!-- ACCESS-FRONTIER-PROBE -->

```python3
from collections import Counter
from itertools import product
from math import inf

def calculate_core_closure_exact(size, arcs):
    d = [[0 if u == v else inf for v in range(size)] for u in range(size)]
    for u, v, w in arcs:
        d[u][v] = min(d[u][v], w)
    for k in range(size):
        for u in range(size):
            for v in range(size):
                d[u][v] = min(d[u][v], d[u][k]+d[k][v])
    return d

def evaluate_seed_closure_exact(d, seeds):
    return [min((w+d[v][y] for v, w, identity in seeds), default=inf)
            for y in range(len(d))]

def merge_access_headers_exact(x, y):
    return (min(x[0], y[0]), min(x[1], y[1]), min(x[2], y[2]),
            max(x[3], y[3]), x[4]+y[4], x[5]+y[5])

parent = [-1, 0, 0, 1, 1]
old = [0, 2, 4, 3, 1]
tin, end, order = [0]*5, [0]*5, []
def visit_route_coordinates_exact(v):
    tin[v] = len(order)
    order.append(v)
    for child in range(1, 5):
        if parent[child] == v:
            visit_route_coordinates_exact(child)
    end[v] = len(order)
visit_route_coordinates_exact(0)
base_length = [0]*5
for v in range(1, 5):
    base_length[v] = base_length[parent[v]]+old[v]
empty = ((inf, -1), inf, 5, 0, 0, 0)
def encode_access_header_exact(record):
    if record is None:
        return empty
    identity, head, weight = record
    if head == 0:
        return ((weight, identity), inf, 5, 0, 0, 1)
    if head >= 5:
        return ((inf, -1), inf, 5, 0, 1, 1)
    return ((inf, -1), weight-base_length[head], tin[head], tin[head]+1, 0, 1)

events, changed = {}, {}
def replace_route_change_exact(v, state):
    before = changed.get(v, (0, 0))
    for x, sign in ((tin[v], 1), (end[v], -1)):
        a, b = events.get(x, (0, 0))
        value = (a+sign*(state[0]-before[0]), b+sign*(state[1]-before[1]))
        if value == (0, 0):
            events.pop(x, None)
        else:
            events[x] = value
    if state == (0, 0):
        changed.pop(v, None)
    else:
        changed[v] = state

def materialize_drift_segments_exact():
    bounds = sorted({0, 5}|set(events))
    signed = failed = 0
    segments = []
    for lo, hi in zip(bounds, bounds[1:]):
        a, b = events.get(lo, (0, 0))
        signed, failed = signed+a, failed+b
        assert failed >= 0
        segments.append((lo, hi, inf if failed else signed))
    assert len(segments) <= 2*len(changed)+1
    return segments

def bound_route_hull_exact(segments, lo, hi, clipped=True):
    if lo >= hi:
        return 0
    value = max(w for a, b, w in segments if a < hi and lo < b)
    return max(0, value) if clipped else value

rows = [None]*8
headers = [empty]*16
row_ids = [300, 301, 101, 302, 102, 303, 304, 305]
def update_access_slot_exact(position, record):
    rows[position] = record
    v = position+8
    headers[v] = encode_access_header_exact(record)
    while v > 1:
        v //= 2
        headers[v] = merge_access_headers_exact(headers[2*v], headers[2*v+1])

def collect_certified_seeds_exact(left, right, segments, mode, pair_drift, counts):
    output, stack = [], [(1, 0, 8)]
    while stack:
        node, lo, hi = stack.pop()
        if right <= lo or hi <= left:
            continue
        a, b, h0, h1, bad, number = headers[node]
        if not number:
            continue
        drift = bound_route_hull_exact(segments, h0, h1, mode != 1)
        if mode == 2 and h0 < h1:
            drift = max(0, max(pair_drift[v] for v in range(5) if h0 <= tin[v] < h1))
        if left <= lo and hi <= right and a[0] < inf and not bad and drift < inf and a[0]+drift <= b:
            output.append((0, a[0], a[1]))
            counts['accepted_cells_'+str(mode)] += 1
        elif hi-lo == 1:
            identity, head, weight = rows[lo]
            output.append((head, weight, identity))
        else:
            mid = (lo+hi)//2
            stack += [(2*node, lo, mid), (2*node+1, mid, hi)]
    return output

counts = Counter()
for generation, states in enumerate(product(range(5), repeat=4)):
    current = {200+v: (parent[v], v, old[v]+1) for v in range(1, 5)}
    current.update({400: (1, 0, 0), 401: (4, 3, 0), 402: (5, 0, 2)})
    for v, state in enumerate(states, 1):
        value = (parent[v], v, old[v])
        if state == 1:
            value = (parent[v], v, 0)
        if state == 2:
            value = (parent[v], v, old[v]+3)
        if state == 3:
            value = None
        if state == 4:
            value = (parent[v], v+1, old[v])
        if value is not None:
            current[100+v] = value
        invalid = value is None or value[:2] != (parent[v], v)
        replace_route_change_exact(v, (0, 1) if invalid else (value[2]-old[v], 0))
    current.update({300: (0, 0, generation % 4), 301: (0, 3, 1+generation % 9),
                    304: (0, 4, (3*generation) % 10), 305: (0, 0, 2)})
    if generation % 7:
        current[302] = (0, 0, generation % 5)
    if generation % 4:
        current[303] = (0, 5 if generation % 3 else 2, 4)
    for i, identity in enumerate(row_ids):
        record = None if identity not in current else (identity, current[identity][1], current[identity][2])
        if rows[i] != record:
            counts['leaf_updates'] += 1
            counts['shared_route_leaf_updates'] += identity in (101, 102)
            update_access_slot_exact(i, record)
    pairs = {}
    for identity, (u, v, w) in current.items():
        pairs[u, v] = min(pairs.get((u, v), (inf, -1)), (w, identity))
    d = calculate_core_closure_exact(6, list(current.values()))
    assert d == calculate_core_closure_exact(6, [(u, v, wi[0]) for (u, v), wi in pairs.items()])
    lengths, pair_lengths = [0]*5, [0]*5
    for v in range(1, 5):
        value = current.get(100+v)
        lengths[v] = inf if value is None or value[:2] != (parent[v], v) else lengths[parent[v]]+value[2]
        pair_lengths[v] = pair_lengths[parent[v]]+pairs.get((parent[v], v), (inf, -1))[0]
        assert pair_lengths[v] <= lengths[v]
    segments = materialize_drift_segments_exact()
    pair_drift = [pair_lengths[v]-base_length[v] for v in range(5)]
    for lo in range(5):
        for hi in range(lo+1, 6):
            expected = max(lengths[v]-base_length[v] for v in range(5) if lo <= tin[v] < hi)
            assert bound_route_hull_exact(segments, lo, hi, False) == expected
            assert bound_route_hull_exact(segments, lo, hi) == max(0, expected)
            counts['drift_hulls'] += 1
    for lo in range(8):
        for hi in range(lo+1, 9):
            permitted = {row[0]: row for row in rows[lo:hi] if row is not None}
            seeds = [(head, w, identity) for identity, head, w in permitted.values()]
            truth = evaluate_seed_closure_exact(d, seeds)
            totals = []
            for mode in range(3):
                output = collect_certified_seeds_exact(lo, hi, segments, mode, pair_drift, counts)
                assert all(permitted[identity] == (identity, head, w) for head, w, identity in output)
                assert evaluate_seed_closure_exact(d, output) == truth
                totals.append(len(output))
                counts['closure_checks'] += 1
            assert totals[1] <= totals[0] and totals[2] <= totals[0]
            counts['signed_strict_ranges'] += totals[1] < totals[0]
            counts['pair_strict_ranges'] += totals[2] < totals[0]
            counts['source_ranges'] += 1
    counts['snapshots'] += 1
print('independent_access', dict(sorted(counts.items())), 'mismatches=0')

# Each mutation below violates one specific premise, not the published algorithm.
d = calculate_core_closure_exact(2, [(0, 1, 1)])
assert evaluate_seed_closure_exact(d, [(1, 2, 9)]) != evaluate_seed_closure_exact(d, [(0, 0, 8)])
d = calculate_core_closure_exact(2, [(0, 1, 0)])
pruned = calculate_core_closure_exact(2, [])
assert evaluate_seed_closure_exact(d, [(0, 0, 8), (1, 0, 9)]) != \
       evaluate_seed_closure_exact(pruned, [(0, 0, 8)])
d = calculate_core_closure_exact(3, [(0, 1, 4), (1, 2, 2)])
assert 1 <= 4-2 < 4  # Last-batch drift 1 passes; cumulative drift 4 rejects.
assert evaluate_seed_closure_exact(d, [(0, 0, 8), (2, 4, 9)])[2] == 4
assert evaluate_seed_closure_exact(d, [(0, 0, 8)])[2] == 6
d = calculate_core_closure_exact(2, [(0, 1, 1)])
assert evaluate_seed_closure_exact(d, [(0, 7, 8), (1, 2, 9)]) != \
       evaluate_seed_closure_exact(d, [(0, 0, 8)])
print('mutation_checks', 'partial_leader/core_pruning/last_batch/stale_leader=detected')

# Signed improvement and a selected-ID failure with a reverified pair alternative.
assert not (5+max(0, -3) <= 2) and 5-3 <= 2
d = calculate_core_closure_exact(2, [(0, 1, 0)])
assert evaluate_seed_closure_exact(d, [(0, 5, 8), (1, 5, 9)]) == \
       evaluate_seed_closure_exact(d, [(0, 5, 8)])
d = calculate_core_closure_exact(2, [(0, 1, 2)])
assert evaluate_seed_closure_exact(d, [(0, 0, 8), (1, 3, 9)]) == \
       evaluate_seed_closure_exact(d, [(0, 0, 8)])
print('strict_extensions', 'signed_decrease=passed', 'reverified_parallel_route=passed')

# A factorized transit-access comparator for the explicit family and route edits.
K, k, step = 32, 31, 100
offsets = [step*i for i in range(K)]
profile = list(range(1, k+1))
matrices = [list(profile), [w+int(j == 0) for j, w in enumerate(profile)],
            [w+2 for w in profile], [0]*k]
family = Counter()
for weights in matrices:
    assert max(w-j for w, j in zip(weights, profile)) <= 2
    for i in range(K):
        for start_at_second_tail in (False, True):
            full = []
            if not start_at_second_tail:
                full.append((0, offsets[i]))
            full += [(j, offsets[i]+j+2) for j in profile]
            for later in range(i+1, K):
                full.append((0, offsets[later]))
                full += [(j, offsets[later]+j+2) for j in profile]
            truth = [min((cost for head, cost in full if head == 0), default=inf)]
            truth += [min(cost+(weights[j-1] if head == 0 else 0)
                          for head, cost in full if head in (0, j)) for j in profile]
            if start_at_second_tail:
                answer = [offsets[i+1] if i+1 < K else inf]+[offsets[i]+j+2 for j in profile]
            else:
                answer = [offsets[i]]+[offsets[i]+w for w in weights]
                family['single_transit_accesses'] += 1
            assert answer == truth
            family['source_checks'] += 1
print('factored_family', dict(sorted(family.items())), 'mismatches=0')

# Identical headers and identical exact Q can hide different slack/head pairing.
same_core = calculate_core_closure_exact(4, [(0, 1, 2), (0, 2, 1),
                                            (3, 0, 0), (3, 1, 0), (3, 2, 0)])
good, bad = [(0, 0, 8), (1, 2, 9), (2, 1, 10)], [(0, 0, 8), (1, 1, 9), (2, 2, 10)]
def summarize_star_fixture_exact(records):
    heads = [v for v, w, identity in records if v]
    return (min((w, identity) for v, w, identity in records if not v),
            min(w-1 for v, w, identity in records if v),
            min(heads), max(heads)+1, 0, len(records))
assert summarize_star_fixture_exact(good) == summarize_star_fixture_exact(bad)
assert evaluate_seed_closure_exact(same_core, good) == evaluate_seed_closure_exact(same_core, good[:1])
assert evaluate_seed_closure_exact(same_core, bad) != evaluate_seed_closure_exact(same_core, bad[:1])
print('summary_indistinguishability', 'same_a_b_hull_core', 'only_one_cell_is_dominated')

# Clear cumulative changes, fail nested edges, then restore just the parent.
for v in range(1, 5):
    replace_route_change_exact(v, (0, 0))
replace_route_change_exact(1, (0, 1))
replace_route_change_exact(3, (0, 1))
replace_route_change_exact(1, (2, 0))
assert bound_route_hull_exact(materialize_drift_segments_exact(), tin[3], end[3]) == inf
replace_route_change_exact(3, (-3, 0))
assert bound_route_hull_exact(materialize_drift_segments_exact(), tin[3], end[3], False) == -1
print('nested_restoration', 'child_failure_preserved', 'signed_cancellation=-1')
```

**AF4 provenance qualification:** the O(K+k) additional metadata includes the b-leader and star-route witness IDs, not a compressed encoding of every arbitrary original x_i->p_j edge ID. Those IDs remain in the common retained ledger. Retrieving k direct-edge witnesses for an x-source answer can read those k records and must be charged; the probe verifies distances, not full original-path unpacking. This does not change the one-access-per-b comparison.

**Follow-on verification:** the appended `python3` block alone replayed with exit status 0 and the recorded output. The original 44,865-byte prefix and the author manuscript matched their recorded hashes. ASCII, trailing-whitespace, fence-balance and all 16 local-link checks passed. No earlier probe was executed.


## Follow-On: Watched Certificate Debits

Date: 2026-09-20. Independent review of the final current-owner / mapped-internal-record revision of [Connectivity-Watched-Debits.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:22). This append does not reopen A02 forest-fragment compression, the earlier A03 elimination review, or the A03 access-frontier review. No author probe or earlier review suite was executed.

Reviewed author SHA-256: `9bdb8f01b2215c40375bc93083023b1e8825a706ccc167446696fb7bca76dd47`.
Pre-append review: 73,139 bytes, 1,021 lines, SHA-256 `1ec156e84bf73ce5957345e0189b3e32ecadab4b8717d83b3f18ebee923c2688`.

**Disposition:** retain the mechanism as a sound, bounded monotone validation adapter, not a demonstrated new connectivity algorithm. I found no counterexample to the final cut lemma, committed owner invariant, or `O(q+K+J+(sigma+T)log(q+1))` control-work bound under its stated assumptions. There is a further safe unread-suffix tightening. Ready-only closure is strictly weaker than paid certificate probing; the graph family admits materially stronger baselines than frozen raw fallback. These are bounded mathematical findings, not demands for production infrastructure.

### WD1: The Final Owner Invariant And Bound Survive

**Locations:** [cut argument](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:30), [owner schedule and bound](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:59), [internal-record boundary](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:28).

The cut proof remains valid when B includes actual edges learned from the extent being certified or from another certificate. A cut separating components of B union (H_i-D) has no crossing B edge or surviving H_i edge. If a surviving E_i edge crosses, the residual-forest property forces either that same edge into H_i, a contradiction, or at least k_i missing H_i edges across the cut. Each is cross-component already in B, contradicting d_i(B)<k_i. Only actual edges enter B; no future replacement is assumed. This is a sufficiency proof, not a necessary condition for replacing an extent.

The revised endpoint-owner argument is correct, with a temporal qualification: owner=DSU-root is a **committed-state** invariant, not an assertion between individual handle moves and the deferred parent commit. During a union S into L:

1. Before the transaction, no live watch has both endpoints in S, because watches are cross-component.
2. Each watch visited through S therefore has exactly one S handle. Its peer is not another handle awaiting processing in S.
3. If the peer owns L, deleting both handles retires the now-internal watch. Otherwise only the popped handle's owner changes to L; the peer stays correct.
4. Once S's list is empty, no live handle owns S. Committing parent[S]=L restores root equality for every remaining watch, including original endpoints several DSU levels below S.
5. Refills and ready releases run after that commit. New owner fields are current roots, and releases unlink both handles through stored owners without root searches.

This proves O(1) peer lookup is legitimate without assuming root labels stay stable forever. The unchanged root's handles need no writes. The author incorporated owner caching during this review; it is **not an outstanding correction or an independent novelty claim**.

Late creation does not invalidate doubling. If endpoint handle h is born in a component containing s_h initial fragments, every subsequent move at least doubles that component's size. Hence

```text
moves(h) <= floor(log2(q / s_h)) <= floor(log2 q), for q >= 1.
```

Growth while the handle stays at the larger root only helps. Retirement and ready release delete each handle once; they need not be charged as another move. At most 2*sigma handles are created. Accounting is over all creations, **not only the initially occupied K slots**. Two size-balanced root searches per consumed record, two per attempted actual-edge union, and O(sigma log(q+1)) handle processing give the revised bound. T includes no-op unions and repeated already-connected payload edges. K+J covers admitted slot/descriptor setup; endpoint mapping and index construction remain separate as stated.

For q=1, internal records cost constant work each. For q=0, there are no legal active-endpoint records or union attempts; empty streams become ready using descriptor work. Paid proof that suppressed implicit-component records were internal is necessary and now explicit. This boundary is resolved, not an open defect.

The independent test exhausts 31,250 (extent, k, deletion-set, actual-known-subgraph) states on four vertices and 75,000 permissible one-edge actual unions. All 21,109 accepted cut tests preserve the partition. It also finds 8,606 valid replacements rejected by this sufficient guard, and 842 failures of the deliberately weakened d_i<=k_i rule. These are boundaries of stronger interpretations, not failures of the published strict rule.

### WD2: Safe Early Release Without Exhausting The Suffix

**Locations:** [refill and release](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:44), [atomic publication](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:63).

Published release at stream exhaustion is safe: once d_i<k_i, further actual unions cannot invalidate it. Remaining cross-component watches can be unlinked even though their endpoints have not merged. They are no longer needed to detect a transition back to unsafe, which is impossible within the pinned solve.

A further tightening uses the already paid exact stream length. Let w be live watches, m the retained missing-stream length, and p the cursor. For an unready guard at a committed state, scanned nonwatched records are internal, so

```text
w <= d_i(B) <= w + (m-p).

w == k_i              => unsafe.
w + (m-p) < k_i       => ready; release watches and abandon this suffix.
otherwise             => continue the existing forward refill.
```

Omitted initial-internal occurrences contribute zero. No sampling or unproved omission is allowed. This still decides the original guard exactly after refill: it changes the stopping proof, not the graph certificate. The ready cursor becomes terminal for this snapshot even if not at EOF.

Concrete post-union case: k=2; the first two records are distinct missing identities between fragments 0 and 1; the third is a missing edge between 2 and 3. The first two are watched. An actual 0-1 connection retires both; only one unread record remains, so d<=1<2 without opening it. This skips a genuinely cross-component suffix record. The elementary initial case m<k also needs no stream reads.

Across 120 independent union schedules, this modification saves 308 occurrence reads with identical readiness at every checkpoint. Saving on one guard's release is at most k_i-1 unread records; this does not create a worst-case asymptotic improvement beyond the existing K allowance. It is a conventional cardinality upper bound, not a new graph theorem.

Atomicity matters for both versions. Refilling **before** parent commit can recreate a watch on just-merged endpoints; that stale watch may never receive another retirement event. Releasing merely because one watch retired, without EOF or the upper-bound proof, is unsound. A graph witness is E={01,12,02}, H={01,12}, k=1, D={01,12}, with current actual B={03,31}. Missing 01 has become internal, but unread 12 remains cross-component. Premature readiness would discharge E using its empty surviving H and miss actual edge 02, yielding the wrong partition. The probe checks this latter boundary. Neither mutation occurs in the latest author schedule.

### WD3: Quiescence Is Not Completion; Certificate Probes Change The Base

**Locations:** [invalid certificate probes](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:91), [fixed-point statement](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:95).

Order independence holds for fixed initial B, portfolio and snapshot, fixed external actual edges already admitted, and fair processing of every newly ready certificate's complete surviving payload. Define F(P) to join P with surviving H_i edges for every i with d_i(P)<k_i. F is monotone and inflationary on the finite partition lattice. Every fair sequence of those actions reaches the same least fixed point above the base. Each action can be marked fired once; at most J firings are needed, including payloads adding no union. This proves neither optimal scheduling nor equality between that fixed point and the full graph partition.

Here is a simple-graph portfolio where they differ. Vertices are 0,...,6; vertex 6 is isolated. Both certificates have k=1 and equal their extents, each already a forest:

```text
E1 = H1 = {01 [missing], 24 [live], 43 [live]}
E2 = H2 = {23 [missing], 05 [live], 51 [live]}
F0      = {01,05,23,24}
B0      = F0-D = {05,24}.
```

F0 is a genuine spanning forest of E1 union E2. Both initial debits are one, so ready-only execution stops with five fragments. Pay to read surviving H1: 24 is already known; 43 actually joins 2 and 3. H2 becomes ready. Reading 05 and 51 joins 0 and 1, so H1 becomes ready too. Final components are {0,1,5}, {2,3,4}, {6}. Both extents can be discharged after their payloads have been processed. Four surviving certificate-edge records, including two already-known edges, were processed; missing-index/access costs are additional. No raw extent is needed.

This is not hypothetical mutual justification: the first invalid certificate was explicitly paid for and its **real** edge performed the enabling union. It establishes a strict closure advantage for certificate probing over ready-only execution on a valid original-forest setup. It establishes no speed advantage over tuned connectivity methods.

**Candidate wording correction:** replace "additional raw probes ... may change how quickly that closure is reached" with "raw or invalid-certificate probes change the base partition and may change the ready-only fixed point itself; for a fixed set of such actual edges, exhaustive closure is order-independent." Complete covering solves still return the same full partition by the discharge lemma. Distinguish that final-answer invariance from quiescent partial knowledge. The probe explores every reachable ready-action order for 64 further graph-derived portfolios, totaling 1,786 states; each has one terminal partition.

### WD4: No New Algorithmic Separation Has Been Established

**Locations:** [experiments](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:97), [prior-art qualification](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:117), [full counters](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Watched-Debits.md:440).

**Known watched constraints.** The independent reduction is to a monotone threshold monitor: x_e(B) says a missing occurrence's endpoints remain separate; only 1->0 transitions occur, and unsafe means sum(x_e)>=k_i. Store k_i surviving supports and reconsider only when a support becomes false. The root-owner adapter supplies those events without registering every occurrence. Minion's sum propagator watches c+1 supports because it also performs stronger domain propagation; our k-watch deficit detector is not a claim that its code is identical. Watched supports and dynamic triggers are already in [Chaff, Section 2](https://www.princeton.edu/~chaff/publication/DAC2001v56.pdf) and [Gent et al., Sections 2-3/Figure 1](https://heather.cafe/publications/papers/gent2006watched.pdf). The graph adapter and paid index lifecycle remain implementation content. I did not find a primary source giving the whole adapter verbatim; that absence does not establish novelty.

**Current sparse certificates.** A maintained current root spanning certificate can represent the answer without checking these stale guards. Strong-certificate composition and maintained sparse hierarchies are developed in [Eppstein, Galil and Italiano, Section 3](https://ics.uci.edu/~eppstein/pubs/EppGalIta-TR-93-20.pdf). Updating it costs work, but frozen initial-frontier fallback is not a substitute for this comparator.

**Direct sensitivity, not vertex-failure confusion.** [Patrascu and Thorup, Section 1](https://people.csail.mit.edu/mip/papers/edgedel/paper.pdf) provide a deterministic linear-space edge-update sensitivity representation with component statistics and find queries. Polynomial construction incurs the sparsest-cut approximation factor: the cited square-root-log approximation gives O(d log^(5/2)n log log n) recovery, rather than the stronger bound with hard exact preprocessing. [Duan and Pettie, Theorem 7.9/Section 7.4](https://arxiv.org/pdf/1607.06865) give an edge-failure oracle with O(n log^2 n) words, expected O(d log d log log n) update time and high-probability correctness. Their forest Euler intervals, deletion-adjusted sketches and actual component reconnection address the graph problem, not merely guard evaluation. Neither RAM theorem is automatically a deterministic 4 GB external-memory implementation. Preprocessing, full vertex labels and original-edge witnesses require separate accounting.

On the author's disjoint-extent family, d equals missing occurrence count and is O(rw), while its selected whole-extent probe costs Theta(w^2/r) plus those occurrences. At fixed r and growing w, sensitivity methods avoid the quadratic raw term up to logarithms under their own preparation/space/error models. At r around sqrt(w), the proposed O(w^(3/2)) record envelope still establishes no improved sensitivity exponent. This is a parameter comparison, not a benchmark or universal dominance claim under the author's limits.

Two simpler matched-family comparators narrow the evidence further:

1. **Paid partial raw probe.** Every extent in that family has a surviving A-B edge. At most |D intersect E_i|+1 raw records need be scanned to encounter one, regardless of order; the deleted B-C edge counts too. Every deleted edge in this extent lies in its k=2 certificate, so that prefix is O(w), not Theta(w^2/r). After this real union, the same watched guards can discharge the first extent as well as the others with empty surviving payloads. Charge prefix reads, deletion membership work, required occurrences and pages. This gives O(w+rw) logical records when partial extent reads are permitted. If extents are indivisible I/O units, declare that restriction. Full-first-extent charging is conservative, not an adaptive-access lower bound.
2. **Fixed-fragment current-cut counts.** On this promised family, intact internal stars prove the base groups A, B, C. A paid initial scan stores cross-group edge counts. Apply all d actual deletion receipts to those counters. A-B stays positive; all C-crossing counts become zero, already proving the changed two-component answer. A paid prefix scan supplies an actual A-B witness. This uses O(1) counters after ownership is known and O(d) receipt work. It is a deterministic comparator for this family, not an index for arbitrary future forest cuts or a free change log, and does not need the M watch-occurrence index.

The reported 65,544->8,193 raw reduction plus 7,944 occurrence reads is consequently narrower than a general access separation. Its full 16,137 count was already acknowledged; there is no reason to relabel it an eightfold total speedup. No previous large-family fixture was replayed here.

**Specific disposition:** accept the final cut/owner/monotone-solve kernel as mathematically supported; optionally adopt WD2 and clarify WD3; retain it as an A02 validation/scheduling component. Do not promote it to the requested differentiated paper-worthy graph algorithm on these experiments. The bounded next scientific comparison is partial-raw plus certificate probing against a current sparse/sensitivity solver with matched paid preparation, output and resident-state allowance, not a demand to build all production engineering.

### Independent Probe Receipt

The new standalone program imports no author implementation, reads/writes no files, and executes no previous probe. It compares the owner-aware variant and WD2 with an independent graph-traversal oracle. Its history list, exhaustive masks and oracle inputs are **test-only resident data**, not a production memory implementation. Owner-stress streams are threshold fixtures with distinct parallel identities and shared identities across certificates. The separate cut test and WD3 fixture use actual residual-forest certificates.

The cut sweep uses all E subset K4, all D subset E, k in {1,2}, and all B subset K4-D: a larger actual-subgraph domain than one prescribed F0. The 4,800 checks count two guard engines over 120 schedules and five checkpoints, not 4,800 independent graphs. The late-birth test moves a handle born at size 2 exactly three times at q=17. Movement is checked per endpoint, not just through a loose total inequality. Initial-internal and q=0 boundaries are included. The ready-only search exhausts reachable action states, not arbitrary raw-probe strategies.

Executed source, exit 0:

```text
cut_audit {'accepted': 21109, 'actual_union_checks': 75000, 'nonstrict_mutation_failures': 842, 'states': 31250, 'sufficient_only_rejections': 8606} mismatches=0
owner_audit {'attempts': 960, 'consumed': 2416, 'created': 1256, 'find_calls': 6752, 'fixtures': 120, 'guard_checks': 4800, 'internal': 1160, 'moved': 528, 'refills': 1556, 'retired': 848, 'unread_released': 308} saved_records 308 mismatches=0
late_birth size=2 moves=3 q=17; q_zero=passed
fixed_point {'portfolios': 64, 'reachable_states': 1786} order_mismatches=0
boundary_witnesses invalid_payload_probe=4_live_edges; premature_release=wrong_partition; tail_bound=skips_1
```

The first development execution passed; a subsequent fixture correction explicitly supplied F0 and mapped its surviving components before executing the invalid-certificate example. The final source below, including that mapping, also passed with this receipt. Maintenance lookups are exactly 6,752=2*(2,416+960); reference traversal does not call the maintenance finder.

### Retained Watched-Debit Probe

<!-- WATCHED-DEBITS-INDEPENDENT-PROBE-START -->
```python
from collections import Counter
from functools import lru_cache
from itertools import combinations, permutations
from random import Random

def compute_oracle_partition_labels(n, edges):
    rows = [set() for _ in range(n)]
    for u, v in edges:
        rows[u].add(v)
        rows[v].add(u)
    labels = [-1]*n
    for root in range(n):
        if labels[root] != -1:
            continue
        labels[root] = root
        stack = [root]
        while stack:
            for v in rows[stack.pop()]:
                if labels[v] == -1:
                    labels[v] = root
                    stack.append(v)
    return tuple(labels)

def enumerate_mask_subsets_exact(mask):
    sub = mask
    while True:
        yield sub
        if not sub:
            break
        sub = (sub-1) & mask

def create_residual_certificate_mask(n, pairs, extent, k):
    selected = 0
    for _ in range(k):
        forest = []
        for j, (u, v) in enumerate(pairs):
            if (extent & ~selected) >> j & 1:
                labels = compute_oracle_partition_labels(n, forest)
                if labels[u] != labels[v]:
                    forest.append((u, v))
                    selected |= 1 << j
    return selected

# Exhaust all simple K4 extents, their failures, and actual known subgraphs.
pairs = list(combinations(range(4), 2))
@lru_cache(None)
def compute_mask_partition_cached(mask):
    return compute_oracle_partition_labels(4, [e for j, e in enumerate(pairs) if mask >> j & 1])

cuts = Counter()
for extent in range(64):
    for k in (1, 2):
        cert = create_residual_certificate_mask(4, pairs, extent, k)
        for deleted in enumerate_mask_subsets_exact(extent):
            missing = cert & deleted
            live = 63 & ~deleted
            for known in enumerate_mask_subsets_exact(live):
                labels = compute_mask_partition_cached(known)
                debit = sum(labels[u] != labels[v] for j, (u, v) in enumerate(pairs)
                            if missing >> j & 1)
                equivalent = (compute_mask_partition_cached(known | (cert & ~deleted)) ==
                              compute_mask_partition_cached(known | (extent & ~deleted)))
                cuts['states'] += 1
                if debit < k:
                    assert equivalent
                    cuts['accepted'] += 1
                elif equivalent:
                    cuts['sufficient_only_rejections'] += 1
                if debit == k and not equivalent:
                    cuts['nonstrict_mutation_failures'] += 1
                for j, (u, v) in enumerate(pairs):
                    if (live & ~known) >> j & 1:
                        after = compute_mask_partition_cached(known | (1 << j))
                        next_debit = sum(after[x] != after[y] for h, (x, y) in enumerate(pairs)
                                         if missing >> h & 1)
                        assert next_debit <= debit
                        cuts['actual_union_checks'] += 1
print('cut_audit', dict(sorted(cuts.items())), 'mismatches=0')

class IndependentOwnerWatchMachine:
    def __init__(self, q, streams, capacities, tail_bound=False):
        self.q, self.streams, self.cap = q, streams, capacities
        self.parent, self.size = list(range(q)), [1]*q
        self.lists, self.slots = [set() for _ in range(q)], [set() for _ in streams]
        self.cursor, self.ready = [0]*len(streams), [False]*len(streams)
        self.watches, self.serial, self.stats = {}, 0, Counter()
        self.tail_bound, self.known = tail_bound, []
        self.history = []
        for c in range(len(streams)):
            assert capacities[c] >= 1
            assert len({row[0] for row in streams[c]}) == len(streams[c])
            self.refill_pending_guard_slots(c)

    def find_current_owner_root(self, u):
        self.stats['find_calls'] += 1
        while self.parent[u] != u:
            u = self.parent[u]
        return u

    def erase_existing_watch_slots(self, wid):
        rec = self.watches.pop(wid)
        for side, owner in enumerate(rec['owners']):
            self.lists[owner].remove((wid, side))
        self.slots[rec['c']].remove(wid)

    def refill_pending_guard_slots(self, c):
        assert not self.ready[c]
        self.stats['refills'] += 1
        while len(self.slots[c]) < self.cap[c]:
            remaining = len(self.streams[c])-self.cursor[c]
            if not remaining or (self.tail_bound and len(self.slots[c])+remaining < self.cap[c]):
                self.ready[c] = True
                self.stats['unread_released'] += remaining
                while self.slots[c]:
                    self.erase_existing_watch_slots(next(iter(self.slots[c])))
                return
            _, u, v = self.streams[c][self.cursor[c]]
            self.cursor[c] += 1
            self.stats['consumed'] += 1
            a, b = self.find_current_owner_root(u), self.find_current_owner_root(v)
            if a == b:
                self.stats['internal'] += 1
                continue
            wid = self.serial
            self.serial += 1
            rec = dict(c=c, endpoints=(u, v), owners=[a, b],
                       births=[self.size[a], self.size[b]], moves=[0, 0])
            self.watches[wid] = rec
            self.history.append(rec)  # Test-only movement history, not resident algorithm state.
            self.slots[c].add(wid)
            self.lists[a].add((wid, 0))
            self.lists[b].add((wid, 1))
            self.stats['created'] += 1

    def merge_actual_edge_endpoints(self, u, v):
        self.stats['attempts'] += 1
        self.known.append((u, v))
        a, b = self.find_current_owner_root(u), self.find_current_owner_root(v)
        if a == b:
            return
        if self.size[a] > self.size[b]:
            a, b = b, a
        pending = set()
        while self.lists[a]:
            wid, side = self.lists[a].pop()
            rec = self.watches[wid]
            assert rec['owners'][side] == a
            peer = rec['owners'][1-side]
            assert peer != a
            if peer == b:
                self.lists[b].remove((wid, 1-side))
                self.slots[rec['c']].remove(wid)
                del self.watches[wid]
                pending.add(rec['c'])
                self.stats['retired'] += 1
            else:
                self.lists[b].add((wid, side))
                rec['owners'][side] = b
                rec['moves'][side] += 1
                self.stats['moved'] += 1
        self.parent[a] = b
        self.size[b] += self.size[a]
        for c in sorted(pending):
            self.refill_pending_guard_slots(c)

    def verify_committed_machine_state(self):
        labels = compute_oracle_partition_labels(self.q, self.known)
        def resolve_reference_root_only(u):
            while self.parent[u] != u:
                u = self.parent[u]
            return u
        for c, stream in enumerate(self.streams):
            debit = sum(labels[u] != labels[v] for _, u, v in stream)
            assert self.ready[c] == (debit < self.cap[c])
            assert len(self.slots[c]) == (0 if self.ready[c] else self.cap[c])
            self.stats['guard_checks'] += 1
        for wid, rec in self.watches.items():
            u, v = rec['endpoints']
            assert labels[u] != labels[v]
            assert rec['owners'] == [resolve_reference_root_only(u), resolve_reference_root_only(v)]
            for side, owner in enumerate(rec['owners']):
                assert (wid, side) in self.lists[owner]
        assert sum(map(len, self.lists)) == 2*len(self.watches)
        assert len(self.watches) <= sum(self.cap)
        assert self.stats['find_calls'] == 2*(self.stats['consumed']+self.stats['attempts'])
        for rec in self.history:
            for birth, moves in zip(rec['births'], rec['moves']):
                assert birth*(2**moves) <= self.q

streams = [
    [(0, 0, 0), (1, 0, 1), (2, 1, 0), (3, 0, 2), (4, 2, 3)],
    [(4, 2, 3), (3, 0, 2), (2, 1, 0), (1, 0, 1)],
    [(5, 0, 3), (6, 1, 2)],
    [(7, 4, 4)],
]
totals, saved = Counter(), 0
for order in permutations(pairs, 3):
    base = IndependentOwnerWatchMachine(5, streams, [2, 1, 3, 1])
    fast = IndependentOwnerWatchMachine(5, streams, [2, 1, 3, 1], True)
    for edge in [None, *order, order[0]]:  # Includes an attempted no-op union.
        if edge is not None:
            base.merge_actual_edge_endpoints(*edge)
            fast.merge_actual_edge_endpoints(*edge)
        base.verify_committed_machine_state()
        fast.verify_committed_machine_state()
        assert base.ready == fast.ready
    assert fast.stats['consumed'] <= base.stats['consumed']
    saved += base.stats['consumed']-fast.stats['consumed']
    totals.update(base.stats)
    totals.update(fast.stats)
    totals['fixtures'] += 1
print('owner_audit', dict(sorted(totals.items())), 'saved_records', saved, 'mismatches=0')

# A watch born in a size-2 component moves exactly three times at q=17.
late = IndependentOwnerWatchMachine(17, [[(0, 0, 1), (1, 0, 16)]], [1])
for lo, hi in [(2, 4), (4, 8), (8, 16)]:
    for v in range(lo+1, hi):
        late.merge_actual_edge_endpoints(lo, v)
for edge in [(0, 1), (0, 2), (0, 4), (0, 8)]:
    late.merge_actual_edge_endpoints(*edge)
    late.verify_committed_machine_state()
assert late.history[1]['births'][0] == 2
assert late.history[1]['moves'][0] == 3
late.merge_actual_edge_endpoints(0, 16)
late.verify_committed_machine_state()
assert late.ready == [True]
empty = IndependentOwnerWatchMachine(0, [[]], [1])
empty.verify_committed_machine_state()
print('late_birth', 'size=2 moves=3 q=17; q_zero=passed')

# A post-union ready proof can omit one still-unread cross-component occurrence.
tail = IndependentOwnerWatchMachine(4, [[(0, 0, 1), (1, 1, 0), (2, 2, 3)]], [2], True)
tail.merge_actual_edge_endpoints(0, 1)
tail.verify_committed_machine_state()
assert tail.ready == [True] and tail.cursor == [2] and tail.stats['unread_released'] == 1

# Enumerate all fair ready-action orders, not just a single scheduler.
rng, closure_stats = Random(9021), Counter()
five_pairs = list(combinations(range(5), 2))
@lru_cache(None)
def compute_five_partition_cached(mask):
    return compute_oracle_partition_labels(5, [e for j, e in enumerate(five_pairs) if mask >> j & 1])

for case in range(64):
    extents = [0, 0, 0]
    for j in range(10):
        extents[rng.randrange(3)] |= 1 << j
    deleted, initial = rng.randrange(1024), 0
    for j in range(10):
        if not (deleted >> j & 1) and rng.randrange(2):
            initial |= 1 << j
    portfolio = [(create_residual_certificate_mask(5, five_pairs, e, k), k)
                 for e in extents for k in (1, 2)]
    seen, todo, terminals = set(), [(initial, 0)], set()
    while todo:
        known, fired = todo.pop()
        if (known, fired) in seen:
            continue
        seen.add((known, fired))
        labels, choices = compute_five_partition_cached(known), []
        for i, (cert, k) in enumerate(portfolio):
            debit = sum(labels[u] != labels[v] for j, (u, v) in enumerate(five_pairs)
                        if (cert & deleted) >> j & 1)
            if not (fired >> i & 1) and debit < k:
                choices.append((known | (cert & ~deleted), fired | (1 << i)))
        if choices:
            todo.extend(choices)
        else:
            terminals.add(labels)
    assert len(terminals) == 1
    closure_stats['portfolios'] += 1
    closure_stats['reachable_states'] += len(seen)
print('fixed_point', dict(sorted(closure_stats.items())), 'order_mismatches=0')

# Genuine simple-graph portfolio: invalid certificates can be useful paid probes.
h1, h2 = [(0, 1), (2, 4), (4, 3)], [(2, 3), (0, 5), (5, 1)]
deleted = {(0, 1), (2, 3)}
live1, live2 = h1[1:], h2[1:]
assert len(set(compute_oracle_partition_labels(6, h1))) == 3
assert len(set(compute_oracle_partition_labels(6, h2))) == 3
initial = [(0, 5), (2, 4)]
original_forest = initial + [(0, 1), (2, 3)]
assert compute_oracle_partition_labels(7, original_forest) == compute_oracle_partition_labels(7, h1+h2)
owner = [0, 1, 2, 3, 2, 0, 4]
cycle = IndependentOwnerWatchMachine(5, [[(0, 0, 1)], [(1, 2, 3)]], [1, 1])
assert not any(cycle.ready)  # Ready-only closure leaves five initial fragments.
for u, v in live1:
    cycle.merge_actual_edge_endpoints(owner[u], owner[v])
assert cycle.ready == [False, True]
for u, v in live2:
    cycle.merge_actual_edge_endpoints(owner[u], owner[v])
cycle.verify_committed_machine_state()
assert cycle.ready == [True, True]
mapped = compute_oracle_partition_labels(5, cycle.known)
assert tuple(mapped[owner[v]] for v in range(7)) == (0, 0, 2, 2, 2, 0, 4)
assert compute_oracle_partition_labels(7, initial+live1+live2) == (0, 0, 2, 2, 2, 0, 6)

# Retiring one watch before reading its suffix is not a readiness proof.
bad_release = IndependentOwnerWatchMachine(4, [[(0, 0, 1), (1, 1, 2)]], [1])
bad_release.merge_actual_edge_endpoints(0, 3)
bad_release.merge_actual_edge_endpoints(3, 1)
bad_release.verify_committed_machine_state()
assert not bad_release.ready[0]
assert compute_oracle_partition_labels(4, [(0, 3), (3, 1)]) != (
    compute_oracle_partition_labels(4, [(0, 3), (3, 1), (0, 2)]))
print('boundary_witnesses', 'invalid_payload_probe=4_live_edges; premature_release=wrong_partition; tail_bound=skips_1')

```
<!-- WATCHED-DEBITS-INDEPENDENT-PROBE-END -->

**WD4 accounting qualification:** O(w+rw) denotes raw-payload plus missing-occurrence records, the same narrow counters used in the author's experiment. Deletion-membership index accesses, ownership verification, physical pages and preparation remain additional; a disk membership lookup is not implicitly O(1) I/O. The fixed-group counter comparator uses trusted, complete edge-change receipts after its paid preparation, not a scan of a sampled deletion list. WD3 is a fixed-point boundary example, not another changed-answer performance family.

**Watched-debit verification:** only the program between the new WATCHED-DEBITS markers was replayed from this file, with exit 0 and the receipt above. The complete pre-existing 73,139-byte review prefix, including the A03 access-frontier append, retains SHA-256 `1ec156e84bf73ce5957345e0189b3e32ecadab4b8717d83b3f18ebee923c2688`. The final author file retains the reviewed SHA-256 `9bdb8f01b2215c40375bc93083023b1e8825a706ccc167446696fb7bca76dd47`. ASCII, whitespace, balanced fences and all 27 local links passed. No author file, production code or shared journal was edited; no commits were made.

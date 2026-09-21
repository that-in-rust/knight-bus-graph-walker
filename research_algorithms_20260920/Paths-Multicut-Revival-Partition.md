# Paths: Multiple Forest Cuts Without Duplicate Revival Or Owner Rewrites

Date: 2026-09-21. A03 derivation with completed independent mathematical
review and two replayed finite correctness probes; not performance measured.
Extends [one-cut repair](Paths-Forest-Cut-Repair.md) and
[disjoint canonical rows](Paths-Cut-Partitioned-Rows.md). No general distance
sensitivity oracle, arbitrary-edit support, physical RAM win or priority claim.

## Question And Candidate Result

The existing construction handles one selected forest deletion. Applying it
independently k times can repeatedly report the same omitted raw arc, and
old one-cut assumptions about revived tails become false. The candidate here
handles a whole cut set against one immutable prepared epoch.

Let C be k distinct deleted identities from the selected directed forest F.
For each originally dominated nonforest raw arc e, retain its identity even
when parallel alternatives have the same endpoints. Let r be the number of
distinct omitted arcs whose original F witness crosses at least one cut.
The proposed publication uses O(1+k+r) new words and no rewritten old vertex
owner plane, ledger records or pair-minimum catalogs. Revival reporting is
O(k log(k+2)+k log(D+2)+r), using a paid three-sided reporting primitive on
the D omitted arcs. There is no k*r output term.

The resulting exact query has at most q+k+r+1 entry states, and a canonical
old-ledger partition bounds useful interval/head reports by A, not k*A.
The underlying static indexes, raw input, output, all query work and version
retention remain paid. A large r or q can erase the reduction. These bounds
are conditional on the index profiles detailed below, not executable timing.

## Frozen-Epoch Contract

Use the same exact raw graph G0, fixed feasible potential h, selected acyclic
zero-reduced-cost forest F, old gates P, old owner pi, complete ledger L of A
records, omitted set D0 of D recoverable forest-dominated nonforest arcs, and
fixed original ancestry coordinates as the one-cut note. All roots and heads
of non-dominated exceptions X are in P. L includes X, dormant self-arcs and
selected forest connectors crossing old capsules. Every record keeps original
ID/incarnation and tail/head/cost provenance.

The batch removes only selected forest identities. No raw nonforest deletion,
cost change, insertion, potential change or arbitrary projection change is
covered. A sequence of publications may replace a cumulative cut set within
the same epoch, but k means that whole set, not the latest receipt count;
rebuild/report costs are charged for each such publication. Cut identities
must be validated against the selected forest before publication.

For a cut (parent(c),c), call c its cut child. For any vertex y, define b(y)
as the deepest cut child which is an original ancestor of y, or absent.
This uses the original forest even when several cuts lie on one root path.

## Deepest-Cut Revival Lemma

For an omitted e=(x,y), x is already an original ancestor of y. Its original
forest witness is broken if and only if

```text
b(y) exists AND depth(x) < depth(b(y)).
```

If any cut lies between x and y, the deepest such cut also lies below x.
Conversely the deepest ancestral cut below x is on the original x-to-y path.
Strict inequality matters: an arc starting at the cut child is not broken by
the cut immediately above that tail. Parallel raw alternatives to a deleted
selected forest edge do revive. Nonnegative loops need no revival.

### Disjoint Head Bands

Sort the k cut-child subtree intervals by original preorder and build their
laminar inclusion forest with an ancestor stack. For each cut child c, take
its subtree interval minus the intervals of its immediate cut descendants.
These pieces partition all heads below at least one cut; each piece has
constant deepest cut c. For k>0, at most 2k-c0 nonempty pieces result, where
c0 is the number of top-level cut subtrees: the inclusion forest has k-c0
child links, so summing one plus each child count gives 2k-c0 before empty
pieces are removed. For k=0 there are no bands. The simpler 2k bound remains
valid. Store each piece with threshold depth(c).

Query the immutable omitted-arc index on `(tin(head),depth(tail))` for each
piece `[l,u)` with `depth(tail)<threshold`. Original head ties are retained by
stable record-ID ordering. Because head intervals are disjoint, every revived
record appears in exactly one query. No r-sized seen-ID hash is needed merely
to deduplicate reporting. Storing/ordering the r actual revived records is
still charged to the publication.

Independent per-cut rectangles instead can output a raw arc k times. On a
chain with k cuts and r parallel root-to-bottom omitted arcs they emit k*r
records, whereas the disjoint plan emits r. This is a separation from that
specified repetition, not from the strongest union-reporting index. A
competent generic geometric control can use the same disjoint bands.

## Multicut Entry Closure And Ownership

Let R be the uniquely reported revived set and

```text
N = (cut children union heads(R)) - P,
a = |N| <= k+r,
P* = P union N.
```

All roots of F-C and heads of X union R are now entry states. Every omitted
arc outside R still has a live zero-forest replacement. Thus original
distances in G0-C equal those in `(F-C) union X union R`.

For each original capsule, form the inclusion tree of its new marks N;
for a query also promote source s if absent. Keep old capsule roots as
implicit roots. Define each mark/root region as its old descendant ledger
interval minus immediate marked-child intervals. Remove every deleted
cross-capsule forest connector by its unique ledger-record rank, not its
tail coordinate or endpoint pair. A cut child is still a hole in its original
inclusion parent, but the connector into that child is absent.

Every surviving old ledger record belongs to exactly one current tail region.
Every forest prefix within that region is live. With t=a+sigma marks including
the optional source and ell<=k deleted ledger connectors, the number of
nonempty old-row intervals is at most `q+2t+ell`, and at most `A-ell`.
Only O(t+ell) changed descriptors need be retained: unchanged old rows remain
implicit whole-row defaults. No q-object publication allocation is required.

The prior colored reporting and clipped pair-minimum indexes apply to each
disjoint interval. Total useful reports over settled states obey
`Z_Q <= covered surviving old ledger records <= A-ell`. This does not bound
revisited index cells or physical pages by A; index-query costs remain.

### Correction To The One-Cut Revived-Tail Rule

With multiple cuts, a revived tail need not be reachable from its old owner.
For forest `0->1->2->3`, cut `0->1` and `2->3`, and include raw alternative
`1->3`. That alternative revives, but a macro from old owner 0 invents an
unbroken prefix to 1. It belongs to current owner 1 instead.

Map **both actual endpoints as needed**, preserving the actual head: group R
by the deepest current entry ancestor of its tail, using the same sparse
marked-region lookup. Source promotion can split one current tail region;
retrieve the corresponding sorted-tail R slices rather than duplicating the
old-owner macro. The revived record itself is neither redirected to a fragment
root nor replaced by an old saved witness. This mapping is a paid O(r log(a+2))
step, followed by grouping/sorting; no raw G0 scan is hidden in it.

## Exact Query And Original-Edge Output

Run lexicographic Dijkstra on `P* union {s}` with keys
`(total reduced cost,total original-edge hops)`. Emit:

1. Old-ledger macros from the disjoint regions using actual head reporting
   and interval-clipped per-head minima.
2. Live forest connectors between immediate marked entries, omitting each
   cut-child incoming connector. Old-gate forest connectors remain in L,
   except for explicitly tombstoned cut identities.
3. Revived arcs from their current tail owner to the actual head, with the
   owner's live forest prefix and original arc ID/incarnation.

Every emitted macro is a live original path. Conversely every retained arc
is simulated by its canonical tail owner and closed head. Region partition
and connector coverage therefore give equality with the canonical current
quotient and original primary distances. A target v has distance
`D(current_owner_s(v))+h(v)-h(s)` and a live forest suffix.

Tie-aware ledger minima use `(reduced_cost,depth(tail),stable_ID)`. Original
hop length of a ledger/revived macro from g through tail x is
`depth(x)-depth(g)+1`; forest connectors use the depth difference. Every
nonempty macro has positive hop count. A minimum-cost/minimum-hop retained
walk cannot repeat an original vertex, so lifting predecessor descriptors
gives a simple original-edge shortest path. Minimum hops in the full raw
graph are not promised, since dominated shortcuts were replaced by forest
paths. Full output/forward-path spooling and witness reads remain charged.

## Costs And Honest Comparators

Beyond the immutable paid base, a conservative publication bound is

```text
O(1 + k log(k+2) + k log(D+2) + r
  + (k+r) log(k+r+2) + (k+r) log(A+2)) work,
O(1+k+r) new retained words.
```

This includes cut validation with a declared indexed forest lookup, revival
reporting, head closure, current-owner classification, sparse boundaries,
revived-record ordering and tombstones. If arbitrary ID lookup costs log(n),
add k log(n); do not hide it in a smaller k term. Missing preparation uses
an explicitly paid scan/build or declines the indexed profile.

The constant pays the empty version header when k=r=0. Other non-unit
metadata lookups/joins must likewise be charged: vertex coordinates, old
owners and connector-ID-to-ledger-rank access are paid prepared capabilities,
not free consequences of the theorem. Exact costs, depths, IDs and sums must
fit the declared word model or incur explicit bit-operation costs. The strong
color-reporting term requires the stated static constant-time RMQ profile;
a generic balanced aggregate tree does not automatically give it.

For a source query, sparse promotion/repartition can be rebuilt from the
O(k+r) publication descriptors, not an n-plane. With an indexed heap and
the strong static color-reporting profile, use the conservative bound

```text
O(k+r + log(A+2)
  + I_Q + Z_Q log(A+2)
  + (q+a+sigma+Z_Q+R_Q+B_Q) log(q+a+sigma+2))
+ target lookup and complete path/result delivery,
```

where `R_Q<=r`, `B_Q<=a+sigma`, and `Z_Q<=A-ell`. Mutable solve state is
O(q+a+sigma), plus the sparse descriptors, source/index workspaces and output
buffers. Space for old coordinates/raw records/indexes is not removed.
No physical page-I/O theorem or RAM-byte guarantee follows from word counts.

Strong controls get the same F, h, raw revival index, color/range-min indexes,
snapshot and complete output. A canonical quotient implemented with these
same sparse partitions matches the query/state bounds. General multiple-
failure distance oracles solve a broader failure/query problem and must not
be dismissed using their dense storage as if they were forced baselines.
The contribution candidate is the explicit multicut sparse-publication and
unique-revival composition; standard geometry or contraction alone is not new.

## Falsification Plan

Compare full original-graph shortest distances and every returned original-ID
path against the sparse construction on all cut subsets of small forests.
Cover nested/disjoint/cross-capsule cuts, k=0, all forest edges cut, parallel
raw alternatives, zero cycles, dormant returns, isolated roots and positive
potentials. Check exact revival identity sets, disjoint complete ledger
ownership, no cut in a prefix, and all original targets/source promotions.

Explicit wrong controls: union-only dedup omitted; shallowest rather than
deepest cut; non-strict tail-depth inequality; old-owner revival grouping;
dropping a cut-root hole; excluding every parallel edge with matching endpoints.
Finite reference arrays/oracle scans are correctness aids, not the claimed
compact publisher or an external-memory experiment.

## Completed Review And Finite Evidence

The [independent review](Paths-Multicut-Independent-Review.md) gives a
conditional GO on revival, unique reporting, current ownership, ledger
partition counts and exact/simple original paths. It rejects unconditional
geometry novelty or superiority over equally indexed canonical contraction.
Its sharper band bound and empty-version/metadata accounting are integrated
above. This is mathematical review, not an independent audit of the lead code.

The lead added a separate resident reference implementation in
[multicut_path_partition_probe.py](experiments/multicut_path_partition_probe.py)
and [seven tests](experiments/test_multicut_path_partition.py). The initial
nested-revival test failed because the module was absent; the complete suite
now passes with Python 3.11 and warnings treated as errors. No code was changed
after this passing replay.

| Probe | Fresh replay result | Scope |
| --- | --- | --- |
| Lead finite reference | 7 tests; 4,740 cut views; 20,153 source queries; 60,747 finite original paths | All subsets of seven candidate arcs and all cuts on two four-vertex forests, two potential profiles, 80 seeded small forests, and explicit identity/ownership/invalid-input cases |
| Independent standalone checker | 660 forest/gate/cut cases; 2,622 source solves; 10,562 target checks, including 386 unreachable targets; 10,176 lifted paths | All topologically numbered forests on 1-4 vertices, gate supersets, cuts and sources for its specified generated multigraph family, plus two sparse fixtures |

The independent checker also detects 699 broken old-owner assignments,
339 shallowest-cut failures and 1,093 failures of a non-strict depth predicate.
These counts are checks within its finite family, not independent real-world
incidents or a statistical reliability estimate. Its nonconstant potentials
exercise negative original edges with feasible nonnegative reduced costs;
this extends the mathematical test envelope, not the public nonnegative-input
contract of the main A03 manuscript.

The lead implementation explicitly scans the omitted-arc collection instead
of building the paid three-sided index. It scans ledger slices for colors and
minima, materializes owner/partition maps and full paths, and uses a lazy
duplicate heap. Therefore neither probe implements the sparse publication
bound, external-memory access profile or physical-RAM guarantee. They establish
finite semantic evidence only. All-path materialization is intentionally a
small-fixture oracle, not a proposed production output representation.

Reproduce from the repository root:

```sh
(cd research_algorithms_20260920/experiments && python3 -B -W error test_multicut_path_partition.py)
awk '/^```python$/{p=1;next} p && /^```$/{exit} p{print}' research_algorithms_20260920/Paths-Multicut-Independent-Review.md | python3 -B -W error -
```

## Prior Art And Next Decision

Follow-up: the [completed unit-hop source gate](Paths-Unit-Hop-Source-Eligibility.md)
now executes the eligibility check below. Its exact q optimum leaves most
states and ledger edges on the local file-dependency snapshot, and D=r=0
removes any need for the revival index there. Full indexed construction is
parked for that source; the weighted/zero-cost theorem remains intact.

The review records precisely which portions of the primary sources were
inspected: [multiple-edge replacement paths](https://arxiv.org/abs/2209.07016),
[multiple-failure exact distance oracles](https://arxiv.org/abs/2111.03360),
and [priority search trees](https://doi.org/10.1137/0214021). The first two
allow broader failure/query interfaces; their costs are not forced baselines
for this narrower selected-forest contract. The third supplies the imported
reporting primitive. Neither bounded inspection nor passing tests establish
historical priority for the full integration.

The supported advance over this repository's previous one-cut construction
is an explicitly correct cumulative multicut interface without repeated raw
revival emissions or old-owner rewrites. An equally indexed canonical control
can use the same construction; no exclusive asymptotic advantage is claimed.

The next decisive gate is a useful frozen dependency/security graph with
documented direction, weights and failure semantics. Measure q/n, A/m,
omitted-record count D, cumulative cuts k and actual revivals r before buying
the full index implementation. Compare a complete rebuild, sequential scan,
independent per-cut reports and the disjoint canonical control with identical
input/output contracts. If q or r is large, retain the scan/rebuild option.
Only a paid builder/publisher/query/output experiment can establish a practical
resource advantage. This closes a theorem/probe increment, not the seven-family
innovation goal or the product RAM contract.

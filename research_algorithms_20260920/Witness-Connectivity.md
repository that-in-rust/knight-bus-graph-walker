# Witness Connectivity

Date: 2026-09-20. A02 research candidate, not a production implementation, measured 4 GB result, or global novelty claim.

Follow-on: [Connectivity-Fault-Cover-Quotient.md](Connectivity-Fault-Cover-Quotient.md)
now has an exact prepared-input research executor for native overlapping
cliques with covered pair edits. Its completed independent review and lead
replay support O(F+k) query words and all-ID output. That review also proves
the immutable true-DFS control has at most 2k+F failed tree edges and
2F+3k explicit fragments. Thus native factor access does not create an
exclusive mutable-state advantage; both preparations and repair schedules
must be compared. The original witness/debit route below remains an option.

The [capped execution study](Connectivity-Capped-Execution-Evidence.md) now
supplies a streamed SQLite source, cancellable local searches and one shared
fallback. Separate implementation review and lead replay support its exact
event schedule. Forty-five complete-output measurements expose both the
replay benefit and dense-negative loss; RAM overlaps the ordinary controls.
These are native constructed families, not a general graph or Neo4j claim.

## Premise Check

**Proposed delta: an exact minimum-read frontier of immutable connectivity certificates, coupled optionally to O(tree cuts + live insertions) mutable fragment state.** Deletion capacity is charged only to missing witness edges. The result may contain genuinely split or merged components; this is not D04's unchanged-partition test.

The graph-theoretic ingredients are established sparse certificates and strong-certificate composition. The narrower research contribution is their combination with heterogeneous stored capacities, cumulative witness-hit accounting, and a query-time optimal cover that does not first rebuild a current root certificate. The optimality result below is deliberately limited to a declared laminar, additive read-cost model. It is not an optimal dynamic-connectivity algorithm.

This follows [D01-D06](../research_4gb_20260919/Architecture-Decision-Map.md), the [decision brief](../research_4gb_20260919/Final-Research-Decision-Brief.md), and the [whole workflow](../research_4gb_20260919/End-To-End-Workflow.md). D01 supplies bounded normalized records, D02 physical ownership, D03 may schedule reads, D04 supplies the baseline unchanged-answer gate, D05 needs complete projection changes, and D06 already owns incidence-native WCC. None is renamed as this contribution.

Expert lenses: exact graph algorithms; external-memory execution; snapshot/retention correctness; adversarial prior-art review. Independent challenges and their finite evidence are recorded in [Connectivity-Paths-Independent-Review.md](Connectivity-Paths-Independent-Review.md); they do not establish publication readiness or a physical-budget implementation.

## Contract

Input is a finite directed or undirected snapshot, normalized to an undirected graph for WCC. Ignore direction, retain the declared vertex universe including isolates, ignore loops, and treat parallel edges either as separate stable edge identities or as a canonical pair with an exact live multiplicity. In the latter case only a transition to zero deletes the canonical edge.

An epoch has base graph G0=(V,E0). A current snapshot is G=(V,(E0 minus D) union A), where D is the set of base edge identities currently absent and A contains currently live non-base identities. Repeated deletion messages are not additional failures. A must remove subsequently deleted insertions. Fixed IDs, vertex universe, projection and authorization interpretation are required; changes outside this contract need normalized rebuilding.

Return every vertex with an exact component label, canonicalized by minimum declared vertex ID, and optionally a current spanning forest using original edge identities. Partition equivalence, not incidental union-find root numbers, defines correctness. This is not SCC, biconnectivity, connectivity after arbitrary vertex faults, or an authorization-policy evaluator.

All figures use 4,000,000,000 physical bytes and at most 50,000,000,000 unique retained prepared bytes across pinned generations and the selected portfolio. Source, scratch and output spools have separately admitted finite limits. The provisional 3 GB worker/1 GB other split is unmeasured.

## Candidate Approaches

| Candidate | Attraction | Challenge and decision |
| --- | --- | --- |
| One forest plus old labels | Very small; cheap when no witness disappears | Already D04; cannot compute a split merely by declaring the old answer valid |
| Uniform k-forest certificate for the whole graph | Can compute changed connectivity under failures | Established sparse-certificate application; global expiry and a large persistent certificate may be poor on localized edits |
| Per-page forests rebuilt through a tree | Locality and sparse summaries | Directly overlaps classical sparsification |
| Witness-hit, mixed-depth cover | Keeps valid coarse certificates while refining only unsafe or expensive regions | Retain as a narrow physical algorithm; requires an honest membership-lookup bill and evidence against tuned sparsification |
| Global forest-fragment state plus that cover | Removes n-sized mutable DSU state on sparse epoch changes; unaffected isolates stay on disk | Exact optional route below; forest fragmentation is known sensitivity-oracle art, and original-tree preparation/full output still count |

Random sketches, colored-failure oracles, or hyperedge expansion avoidance are not claimed as inventions here. [Connectivity Labeling in Faulty Colored Graphs](https://arxiv.org/html/2402.12144) already treats correlated color failures, including a near-linear-space centralized single-color oracle. A deleted source group is not automatically one edge failure in the present contract.

## Construction

Partition base edge identities into L bounded raw extents. Their union is E0 and extents do not overlap. Build a fixed binary hierarchy over contiguous extent ranges. A node x represents base edge subset Ex, with nx incident vertices; its logical vertex set also includes all other vertices as isolates.

Optionally retain one or more certificates H(x,k), k>=1:

1. R := Ex; H := empty.
2. Repeat k times: compute a maximal spanning forest F of R; append F to H; remove exactly F from R.
3. Retain H sorted by stable edge ID, with endpoint IDs and an indexed directory. Retain its source epoch, covered extents, k, byte size and checksum.
4. Do not retain it when it saves insufficient bytes or the shared storage cap would be exceeded. Missing certificates simply disable that option.

H has at most k(nx-1) edges for nx>0, and never more than |Ex|. These are residual forests, not k copies of one tree. The sparse-connectivity foundation predates this study; [Nagamochi and Ibaraki, 1992](https://gi.cebitec.uni-bielefeld.de/_media/teaching/2013summer/936nagamochi-ibaraki-1992-sparse-k-connected-subgraph.pdf) gives sparse k-connectivity constructions. The elementary residual-forest version below has its own proof and may use k scans rather than that paper's linear-time construction.

The most conservative bounded builder rereads Ex for each forest and uses one budgeted union-find instance. A disk-backed rank-balanced instance is valid if local vertex state does not fit; it has potentially bad random I/O. Computing high-order parent certificates from arbitrary low-order child forests is NOT allowed: information required by the parent's k may already have been discarded.

## Core Lemma: Witness-Hit Debit

For every cut S, residual forest packing guarantees

```text
|H intersect cut(S)| >= min(k, |Ex intersect cut(S)|).
```

Proof: while any residual edge crosses the cut, its two endpoint components must be connected by a spanning forest path, so that forest has a crossing edge. Across k rounds either all original crossing edges have been taken, or at least k distinct crossing edges have been taken. This also shows that if the original cut has fewer than k edges, every one is in H.

**Sufficient condition, derived here from that established property:**

```text
debit(x,k) = |D intersect H(x,k)| < k
    implies CC(Ex minus D) = CC(H(x,k) minus D).
```

Importantly, |D intersect Ex| can be arbitrarily larger than k.

Proof: H-D is a subgraph of Ex-D, so it cannot invent connectivity. If it separates vertices connected in Ex-D, take an H-D component cut crossed by a surviving original edge. All H edges on that cut were deleted. If the original cut had fewer than k edges, H contained all of them, contradicting the surviving original edge. Otherwise at least k H edges were deleted, contradicting debit<k.

This is a sufficient, not necessary, test. Its sharp failure is easy: use k parallel/cross-cut certificate edges and another excluded crossing edge; delete the k witnesses. The excluded edge still connects the original graph while H-D does not. Never accept equality debit=k merely because the deleted count is small relative to |H|.

For k=1 this includes the ordinary intact-forest condition. For k=2 deleting one actual bridge is allowed: both the certificate and graph split, and the new partition can be computed exactly. The lemma does not promise unchanged labels.

## Minimum-Read Frontier

A legal cover is an antichain of hierarchy nodes covering every raw extent exactly once. At each selected node choose a certificate whose debit is below k, or a raw scan. Raw scanning is always available and filters current deletions. Add live insertions A separately.

Let c(x,k) be the charged read cost of certificate H(x,k), including page rounding and a fixed per-extent open cost. Let r(x) be the corresponding raw read cost. These prices exclude a separately measured common debit-validation phase. The scalar objective can be bytes or a fixed weighted service model, but not an unstated prediction of wall time.

```text
choose_witness_frontier_exact(x):
    best = (r(x), RAW(x))
    for each retained H(x,k):
        if debit(x,k) < k:
            best = cheaper(best, (c(x,k), CERT(x,k)))
    if x has children left,right:
        a = choose_witness_frontier_exact(left)
        b = choose_witness_frontier_exact(right)
        best = cheaper(best, (a.cost+b.cost, SPLIT(a,b)))
    return best

compute_current_partition_exact(root, A):
    validate_complete_epoch_delta()
    compute_cumulative_witness_debits()
    plan = choose_witness_frontier_exact(root)
    initialize_budgeted_union_state()
    for item in plan in extent order:
        stream item edges, excluding D, into union-find
        retain successful original edges if a forest is requested
    stream live A into union-find
    stream all original vertex IDs with canonical component labels
```

Use deterministic tie-breaking by certificate ID. Process the DP postorder; a bounded stack or disk table suffices. Emit the selected frontier on a second traversal, not a RAM-resident list of all selected edges.

**Cover correctness.** Each selected subgraph has exactly its current original subgraph's partition by the lemma, or by raw scanning. Replace any original path segment in one selected subgraph with a certificate path; their union therefore preserves connectivity. Adding the same A on both sides preserves this equivalence. Every emitted witness is an actual current edge.

**Optimality in the declared family.** A cover of a node either selects that node or splits into covers of its children. Those cases are exhaustive and disjoint. Induction proves the recurrence minimizes additive cost. This does not optimize certificate construction, cache sharing, overlapping physical pages, CPU parent locality, or future workloads. Certificates must occupy disjoint charged extents for this additive statement; shared physical pages require a richer cost model.

This is a concrete algorithm, but the generic tree DP is not claimed new. The experimental claim is that the combination produces a cheaper current answer without root-certificate maintenance on sparse snapshot edits.

## Debit Maintenance Is Real Work

A certificate's debit is relative to its own immutable base epoch, not the last query or last published answer. Refreshing an answer does not reset it. This rule is necessary even after an empty change interval.

Option 1, no extra inverted copy: route each changed base edge to its raw leaf and probe the edge-ID indexes of retained ancestor certificates. There are at most O(t log L) probes for t changed base edges when one option is retained per node; multiple k options multiply this count. A B-tree-style index has its own page cost. Increment/decrement the debit only on current absence/presence transitions of that exact base identity. Certificate payload already sorted by ID supports these lookups without a second full edge copy.

Option 2: a retained inverted membership index maps an edge to every certificate occurrence. Its payload is O(sum |H|), not O(m) independent of hierarchy height; charge it explicitly. Do not claim delta-only traffic while silently scanning every certificate to find hits.

If change coverage is uncertain, reconstruct D and A from complete normalized snapshots using external joins. The inexpensive refresh claim is then gone, but correctness is not. No Bloom-filter false negative, stale multiplicity, or ID reuse can be tolerated. The first design keeps a single base epoch; partial reanchoring of individual nodes is deferred to avoid mixed-delta ambiguity.

## RAM And I/O Algebra

Let B be bytes per I/O page, C all retained certificate bytes, S selected certificate/raw bytes after page rounding, a inserted edges, t current change records, z output rows, H hierarchy nodes and J debit-index page transfers.

For the ordinary solver, using rank-balanced union-find with canonical-minimum metadata and 32-bit vertex IDs (the optional fragment solver below replaces this state term):

```text
resident union state = 4n parent + 1n rank + 4n canonical_min = 9n bytes
worker = 9n + runtime + I/O + decode + output + directories + headroom
query edge-read pages <= S/B + ceil(16a/B), plus J and metadata pages
CPU = O((selected_edges+a+n) log n) without path compression
output >= 12z bytes for packed (external_u64, component_u32)
```

This 9n baseline assumes internal IDs are ordered by external canonical ID, so a u32 minimum rank suffices and the ID map can remain on disk. With arbitrary internal ordering, retain actual u64 minima (13n state) or charge external-ID comparison lookups. The optional Euler-ID route explicitly uses u64 minima and does not inherit this ordering assumption. The u32 profile requires n<2^32 with a reserved null encoding where needed.

Union-by-rank depth is at most log2(n), since a rank increment at least doubles represented size. Disk-backed fallback with bounded cached state therefore has the conservative bound O((selected_edges+a+n) log n) state-page transfers, not a sequential bound. An optimized resident DSU is a strong baseline whenever it fits.

At n=200M, resident union state is 1.8 GB. Reserve another 0.30 GB runtime, 0.128 GB I/O, 0.128 GB decoding, 0.032 GB output, 0.064 GB metadata and 0.548 GB headroom: exactly 3 GB. The kernel/OS and all other local processes must actually fit in the remaining physical 1 GB; this is an allocation sketch, not a measured guarantee.

Build traffic for the conservative certificate algorithm is

```text
sum over retained (x,k) of [k * bytes(Ex) + union_state_IO(x,k) + bytes(H(x,k))]
+ endpoint/ID normalization, sorting and validation traffic.
```

A hierarchy with every node retained can repeat the raw edge volume at every level. The retention rule cannot fix excessive build traffic after it has already been incurred. Plan the selected hierarchy first and enforce both preparation and live-disk bounds.

For normalized sort payload X, in-memory run budget M and fan-in f>1, a simple run/merge implementation moves approximately 2X(1+ceil(log_f(ceil(X/M)))) bytes when there is more than one run; one run still needs its input and output. This is a declared implementation count, not a new sorting bound. Input/output runs can coexist; peak scratch must be calculated from the actual deletion schedule.

## Worked Storage Shape

Assume n=200M, m=1B undirected canonical edges, numeric original IDs, and no selected large property columns. These are budgets, not measured encodings.

| Retained object | Decimal GB |
| --- | ---: |
| Base (u32,u32,u64-edge-ID) edges | 16.00 |
| Original-ID map, u64 per vertex | 1.60 |
| Old component column, u32 per vertex | 0.80 |
| Selected certificate portfolio, INCLUDING root and all retained levels | 8.00 |
| One requested forest, up to 16(n-1) bytes | 3.20 |
| Index directories and metadata allowance | 0.25 |
| Current normalized delta allowance | 0.50 |
| Total | 30.35 |

A k=2 root alone can cost 6.4 GB; thus the remaining certificates have only 1.6 GB in this example. The table does not assume all hierarchy levels fit. A separate inverted witness-membership copy is NOT included and must replace another allowance or be added.

A distinct new label column and forest add 4 GB during pinned-reader overlap: 34.35 GB before any additional changed base blocks. Rebuilding all 16 GB base edges would raise that to 50.35 GB and fail the 50 GB retained cap. Use bounded overlays/shared blocks, retire unpinned generations, or defer rebuild; never delete a pinned generation to manufacture a fit. Source staging, sort runs, unsuccessful build outputs and recovery journals are extra peak-disk obligations.

Full output of 200M packed (u64 ID,u32 component) rows is at least 2.4 GB. A u64 canonical external representative makes it 3.2 GB. The consumer must accept a byte-backpressured stream or an admitted spool. A checksum alone is not delivery. Optional forest output adds as much as 3.2 GB.

## Optional State Reduction: Global Forest Fragments

The original route above reduces edge reads but still carries 9n bytes of mutable union state. This optional route replaces that state with a quotient of **one persistent global original spanning forest** F0. It computes changed partitions, not merely the validity of old labels. Euler coordinates are preparation, not a claim of new physical traversal.

### State And Exact Fragment Bound

Choose F0 spanning every original component of G0, with singleton trees for isolates. Root and preorder it once. Retain on disk each vertex's parent, subtree-end coordinate, original-component root and original canonical minimum. Internal IDs below are global preorder positions. Forest edges retain their exact original identities, so deleting one parallel edge does not silently delete a different parent witness.

Let K be the cut-child vertices of the currently missing F0 edges, d=|K|, and a=|A|. These are cumulative differences from the immutable base, not cuts since the last answer. Define active original components as those containing a cut or an endpoint of a live insertion. Let c_active be their count.

The quotient has one initial fragment for every cut child, plus one residual-root fragment for every active original component:

```text
q = d + c_active <= min(n, 2d + 2a).
all fragments, including implicit unaffected trees = c0 + d.
```

The first inequality follows because at most d original components contain cuts and at most 2a contain insertion endpoints. Each original tree cut increases its fragment count by one. This bound depends on failed original **tree** witnesses, not on the total number of non-tree deletions.

Do not allocate c0 entries: c0 can equal n. Untouched roots, old labels, Euler arrays and full output roots stay on disk. If d=a=0, q=0 even for a billion isolates or many absent non-tree edges. Insertion endpoints in the same original component can be deduplicated before state allocation.

### Nested Cuts Without An n-Entry Owner Array

Use half-open original subtree intervals [tin(v),tout(v)). For any vertex v in an active component:

```text
owner(v) = deepest cut child c whose interval contains tin(v),
           or the original component root when no cut contains it.
```

A fragment is not generally one interval. The fragment below c is its original subtree with the subtrees of the nearest nested cut children removed. Its remaining pieces can be separated in preorder.

Build a sorted event stream from the starts/ends of K's subtree intervals and the active component intervals. Sweep each active component, maintaining only a stack of currently containing cut intervals. At a boundary pop every ended interval before pushing a newly starting cut. Emit each nonempty span with the deepest stack entry or the active original root. There are at most 2d+c_active spans, at most 2q. The sweep stack has depth at most d, not original forest height. Map arbitrary edge endpoints by predecessor search in this O(q)-span table, checking the right endpoint to distinguish gaps belonging to inactive components.

Example: original tree edges (0,1),(0,6),(1,2),(1,4),(2,3),(4,5),(6,7),(6,8), with preorder 0..8. Delete (0,1) and (1,2). The spans (start,end,fragment) are:

```text
(0,1,0), (1,2,1), (2,4,2), (4,6,1), (6,9,0).
```

Assigning all of [1,6) to fragment 1 would wrongly absorb fragment 2. Treating its two surviving spans as separate components would wrongly split fragment 1.

### Procedure And Cross-Hierarchy Composition

```text
compute_fragment_partition_exact(F0, D, A):
    validate the immutable epoch and complete current D,A
    obtain cut-child descriptors K using exact F0 edge identities
    fetch and deduplicate active original-root descriptors from disk
    if fragment reservation exceeds the worker budget: use bounded fallback
    sweep cut events into active (lo,hi,fragment) spans
    initialize union-find only on q active fragment IDs
    plan = choose_witness_frontier_exact(root)

    for each surviving edge (u,v) from plan, then each live insertion:
        p = locate_active_fragment_exact(u)
        t = locate_active_fragment_exact(v)
        if p and t are inactive:
            continue  # selected base edge lies inside one intact old tree
        require both endpoints active
        union(p,t); record original edge only when the union succeeds

    initialize q current-root canonical minima to infinity
    for each intact active old component:
        merge its disk-resident old minimum into its current union root
    for original-ID rows in damaged original components, in preorder:
        reduce ID into minimum[find(owner(vertex))]
    stream every vertex in preorder:
        if active: emit (original_ID, minimum[find(owner(vertex))])
        else: emit (original_ID, disk-resident original canonical minimum)
```

A bounded full external-ID scan can replace the damaged-component scan; it changes traffic, not correctness. During ordered scans maintain a span cursor, so owner lookup need not cost a binary search per output vertex. Flatten q union roots once after unions, not n roots.

**All certificates map through the same global F0 spans.** They may cross any original-tree branch and need not contain F0 or respect its fragments. A page-local forest is not an alternative owner definition. No certificate-local component numbers may leak into the quotient.

Proof of exactness has three parts:

1. Removing K partitions F0 into connected subgraphs of the current G; the deepest-cut rule describes those subgraphs exactly.
2. The chosen surviving cover S plus A has the same partition as G by the earlier theorem. Adding F0 minus K cannot create connectivity absent from G. Therefore S union A union (F0 minus K) still has exactly G's partition. Contracting the connected F0 fragments preserves that partition, and streaming edges of S and A into the fragment DSU constructs that contraction.
3. An inactive old component has no missing F0 edge and no insertion endpoint. Its surviving F0 still connects every vertex, and no base edge can leave an original component. It is exactly unchanged and disconnected from active components. Passing its stored labels through is therefore correct.

For canonical minima, first complete every quotient union. Intact active old components contribute their stored minimum; damaged ones contribute every original vertex ID to its actual current union root. Minimum is associative, and these contributions partition the active vertices. Thus the resulting labels are actual minimum external IDs, not Euler numbers or arbitrary DSU representatives. Never use only each cut subtree's original minimum: a nested removed subtree may contain that minimum.

Optional forest output streams F0 minus K followed by successful quotient-union edges. Contracted fragments are trees and successful unions never close a quotient cycle, so this is a valid current spanning forest. Its n-current_component_count edges are streamed, not retained in RAM. It need not become the next epoch's F0.

### Optional Fragment-Relative Certificate Debit

The standalone debit test remains unchanged. Inside the fragment solver only, let B=F0 minus missing original forest edges, and let beta(v) identify a surviving B fragment, including implicit inactive original components. Define

```text
relative_debit(x,k) = count of missing H(x,k) identities (u,v)
                      for which beta(u) != beta(v).

relative_debit(x,k) < k
    => CC(B union (H(x,k) minus D))
       = CC(B union (Ex minus D)).
```

This stronger rule is valid because every cut after contracting B is a union of whole B fragments. Deleted witness edges internal to one fragment cannot cross any such cut. Apply the original strict witness argument on the contracted multigraph, preserving parallel identities; the cut contradiction now counts only missing cross-fragment witnesses. The replacement composes with the same B and any further union, so the heterogeneous antichain frontier remains exact in the B-augmented solver.

**Do not use this predicate in the standalone solver.** For Ex={01,02,12}, H(x,1)={01,02}, global F0={02,12,23}, and D={01,23}, ordinary debit is 1 and rejects. Relative debit is 0 because 0 and 1 remain in one B fragment. H-D alone fails to preserve Ex-D, whereas B union (H-D) is exact. The additional known connectivity is part of the proof, not optional context.

The query procedure first builds the global fragment spans, then evaluates relative debit for candidate certificates and runs the existing minimum-read frontier recurrence using that predicate. Retain the ordinary debit as a cheaper sufficient test. If relative validation costs too much, use the conservative rule or raw fallback; that choice cannot invalidate a correct result.

Relative debit is not an immutable-epoch scalar counter. A tree cut or restoration can move the endpoints of an old deleted witness into different fragments without a new deletion of that witness. Recompute from complete missing-certificate occurrence records and current endpoint ownership, or supply a proved incremental maintenance scheme. The first revision specifies the recomputation only. Endpoint lookup, occurrence scans and their sort/index workspace are charged. The DP minimizes selected-read cost AFTER this separately priced validation; it does not claim to minimize the combined adaptive-discovery cost.

The subsequent [watched-debit extension](Connectivity-Watched-Debits.md) supplies a monotone **within-one-snapshot** validation schedule: at most k current crossing watches per registered certificate, forward-only occurrence cursors, and small-to-large endpoint-handle movement. Direct current-owner fields give proposed O(q+K+J+(sigma+T)log(q+1)) control work and O(q+K+J) logical state, with all missing-index preparation charged. Actual unions can enable certificates and avoid later raw scans even when the final component partition genuinely splits. Its finite probe passed, but independent review and a matched sensitivity-oracle advantage remain open. It does not update DSU through snapshot deletions or inherit the static frontier DP's cost-optimality claim.

### Zero Tree Cuts Need No Base Certificate Reads

Once complete change validation proves that no original F0 tree edge is missing, B already connects every original component. Arbitrary non-tree deletions cannot split them. Skip base-certificate reads, debit evaluation and frontier planning; initialize quotient state only for old component IDs touched by live insertions, union those insertions, and stream canonical labels through those unions. This works when a>0 and components merge, not only when the answer is unchanged. It still pays to establish the complete tree-cut set and insertion set, fetch component minima, and deliver all output.

Both refinements were independently derived and checked in [Connectivity-Paths-Independent-Review.md](Connectivity-Paths-Independent-Review.md): 4,000 complete frontier comparisons included raw options at internal nodes and heterogeneous k, and 1,509 certificate cases were accepted by relative debit but rejected by ordinary debit without an incorrect B-augmented answer. These are small mathematical probes, not an implemented external occurrence index.

### Whole-Cost State Route

An example packed reservation is **128q bytes** for active descriptors, at most 2q spans, cut stack/events, DSU/rank, u64 canonical minima and successful-union witnesses. This is a conservative engineering allowance for flat arrays with in-place sorting, not a measured allocator bound. A separate hard byte counter must include actual capacities. Fixed decoder, source, index, I/O and output buffers remain charged.

```text
R_worker = 128q + fixed_controlled_services + allocator_headroom <= 3 GB
CPU after preparation =
    O(q log(q+1) + (selected_edges+a) log(q+1) + n)
edge pages = ceil(S/B) + ceil(16a/B) + membership/descriptor pages
label traffic with columnar storage =
    8*n_damaged external-ID bytes read
    + 16n original-ID/old-minimum bytes read
    + 16n output bytes written
    + boundary-page overhead
```

The CPU bound uses rank-balanced DSU, O(log q) predecessor lookups on streamed edges, and flattened roots for output. n_damaged is vertices in original components with a tree cut, possibly n after one cut. Intact active components require only O(c_active) indexed old-minimum lookups before the full output pass. Arbitrary original-ID ordered output adds a paid permutation or external sort.

For d=50,000 and a=50,000, q<=200,000 and 128q<=25.6 MB. This replaces 1.8 GB of DSU state at n=200M, **not the whole process with a 25.6 MB process**. Keep the same fixed-service allocations as the resident route and leave the freed space as headroom or explicitly budgeted cache. Huge output and index service can still dominate. At n_damaged=n=200M the displayed label read/write payload is 8 GB; row-interleaved storage can read much more. No n-state array is needed for those scans.

Mapping and deletion accounting are not free:

- Keep full D and A in bounded on-disk normalized streams/indexes. Do not sneak a RAM set of every deletion into the 128q allowance.
- Discover failed F0 identities by merge with its identity-sorted file, or indexed probes on complete change receipts. The scan alternative reads up to 16(n-c0) forest bytes plus D even when q is tiny; probe traffic is charged separately. A previously maintained current K can be reused only with complete subsequent receipts.
- Fetch only cut-child and insertion-endpoint root/Euler descriptors. If normalization does not already use preorder IDs, ID lookup or batched joins are additional work.
- Filter S against D using bounded indexed lookups, or sorted merge where their order permits. An extent hierarchy alone does not imply that D is cheaply joinable.
- Debit/frontier processing also uses bounded metadata pages or external tables. The q-bound is for connectivity state, not permission to load the entire hierarchy.
- Prepare F0, old minima and preorder in the first place. A conservative builder can use bounded cached DSU and an external DFS stack; it may take many random I/Os. Remapping graph endpoints and certificate endpoints requires full joins/sorts. This is a paid base-epoch operation.

For a storage comparison, replace the earlier 8n ID map plus 4n component column with columnar (external_ID:u64, old_minimum_ID:u64, parent:u32, subtree_end:u32, original_root:u32), totaling 28n. Add a 4n inverse lookup only when original IDs form a dense numeric domain; arbitrary IDs need their own measured dictionary. Keep the earlier 16n identity-bearing F0 forest allowance. At n=200M this adds 4.0 GB to the 30.35 GB example: **34.35 GB**, plus at most 16q if quotient witnesses are retained, rather than streamed.

A fresh saved u64 label column adds 1.6 GB while the immutable original labels remain pinned. A separately saved current forest adds up to 3.2 GB; it cannot replace F0 while this epoch still depends on F0. The forest identity index must fit the declared index allowance or increase it. Even a full extra 16 GB raw base copy already exceeds 50 GB from the 34.35 GB base shape, before new answers. Source/sort/build scratch and output spools still require their separate peak-disk admission. All portfolio coexistence must be recomputed by the lead; these are not independent 50 GB entitlements.

Refresh never reanchors F0 or its old labels just because an answer was published. A restored original forest edge removes its cut descriptor; old inserted edges subsequently deleted disappear from A. Rebuild the small quotient from the current cover each query rather than attempting a DSU deletion. If d+a becomes large, interval state can approach n and the 128q reservation can be worse than 9n; select the ordinary resident/paged DSU route or reject the deadline. Reanchoring is a full, separately admitted preparation.

### Prior Art And Honest Surviving Delta

This is **not a new forest-fragment algorithm**. [Duan and Pettie, Section 2, Theorem 2.1 and Corollary 2.2](https://arxiv.org/pdf/1607.06865) explicitly represent failed-tree components by unions of Euler intervals, locate vertices by predecessor queries, and recover connectivity through two-dimensional range reporting. Its Figure 1 even illustrates the interval-with-holes issue. A competent comparator therefore combines that ET structure with our same output/minimum scan; comparing only with an n-state recomputation is insufficient.

[Patrascu and Thorup, Planning for Fast Connectivity Updates](https://people.csail.mit.edu/mip/papers/edgedel/paper.pdf), Sections 1-3, supplies a deterministic batch-connectivity oracle and handles insertions over recovered component IDs. Its expensive expander-hierarchy preprocessing and sparsest-cut approximation factor belong in the lifecycle comparison. Neither paper is a physical-4-GB measurement.

The potentially differentiated combination is now: **global, implicit untouched components + O(d+a) active forest-fragment state + immutable heterogeneous witness-hit cover + streamed canonical results**, choosing certificate reads instead of a mandatory whole-edge range index or dynamically maintained root. This is a concrete state reduction beyond D04, but the graph-state principle is already sensitivity-oracle art. Its novelty, if any, is the exact bounded-storage/read-policy composition. No asymptotic superiority over the ET oracle is proved. ET range reporting can decisively win when d is tiny and its index is affordable.

### New Falsifiers And Changed Design

Initial state claim: "The witness cover is smaller, so connectivity has less state." Challenge: an ordinary cover solver still needs 9n DSU bytes. Revision: contract surviving original-tree fragments before union, and keep only active components.

Second claim: "Store a root for every original tree and one interval per cut." Counterexample: many isolates restore an n-entry table; nested branch cuts create holes. Revision: leave unaffected roots on disk and use the event-sweep union-of-intervals representation.

Third claim: "Use old subtree minima and emit labels in the first pass." Counterexample: nested fragments remove the old minimum, and a later quotient edge can merge fragments. Revision: finish unions, reduce actual original IDs in damaged trees, then perform a separate output pass.

Additional adversaries: one cut in a giant tree makes the minimum scan Theta(n); all parent witnesses missing makes q approach n; adversarial ID order adds sorting; nonwitness deletion floods cost delta validation even with q=0; certificates crossing raw extents are safe only under the single global mapping. The tree/isolate case may gain RAM but still lose preparation time to plain streaming union-find.

## Strong Baselines And Nearest Art

### Whole-Workflow Admission

For either solver, record one source snapshot/CDC boundary and charge wire bytes, decompression, normalization, ID/edge-identity joins, certificate/forest preparation, every query, every delivered row, refresh and recovery. If extraction runs locally, its buffers and database process count against the same physical 4 GB, not against an imaginary second machine. A source projection that expands implicit factors must be compared with D06 before this route is admitted.

```text
T_workload = T_extract + T_normalize + T_prepare
           + sum(T_debit + T_query + T_output + T_refresh + T_recovery)
D_peak_host = D_source_staging + D_unique_prepared_pinned
            + D_live_build_scratch + D_local_outputs + D_journals
D_unique_prepared_pinned <= 50 GB
D_peak_host <= explicitly available local storage
```

Build scratch includes old/new remapping runs and failed unpublished objects until reclaimed. Consumer backpressure needs bounded buffers, an admitted finite spool, or cancellation. Source staging may be omitted only for a truly streamed source with its retry/replay cost included. No dataset byte count or device rate is supplied here, so no numerical end-to-end deadline is certified. The useful-workload test is whether cumulative query savings exceed extra preparation, refresh, output-ordering and retention costs against the same comparator; a small q alone is insufficient.

### Comparators

- Resident rank-balanced DSU over the canonical edge stream, with the same IDs, full output and refresh. At the worked n it is competitive; do not compare only with a needlessly duplicated adjacency projection.
- A single global k-forest certificate, our witness-hit debit rule, and DSU. This ablation tests whether the hierarchy contributes anything.
- A current sparse-certificate tree with eager or batched updates. [Improved Sparsification, Section 3](https://ics.uci.edu/~eppstein/pubs/EppGalIta-TR-93-20.pdf) defines strong certificates, composes children and propagates updates to the root; Section 4 improves partitioning. Therefore sparse summaries, hierarchy, and local repair are already solved. Our claim is only a different immutable-read policy and its lifecycle economics.
- D04 intact forest plus labels, then tuned recomputation on rejection; D06 incidence-native union where source factors exist. Paying to expand cliques would be an unfair comparator.
- GDS WCC with the declared projection/output and hardware budget, plus a larger-machine economics comparator. Vendor timing claims are not measurements in this study.

**Novelty verdict:** neither sparse certificates nor forest-fragment state survives as a new graph-theoretic invention. Their witness-hit, heterogeneous-frontier, implicit-component composition is potentially differentiated beyond D04, but may be a straightforward specialization of sparsification and sensitivity oracles. Evidence of a workload-level advantage and a broader literature check are prerequisites to a research-paper claim.

## Adversarial Cases

1. A tree: every edge is a witness, and certificates provide no topology compression. Raw DSU wins on build and metadata.
2. Expander-like shards: most local vertices and many certificate edges remain; hierarchy copies consume disk without reducing useful reads.
3. Delete exactly k certificate edges across one cut while an excluded edge survives: the strict debit test rejects. Relaxing it silently produces a false split.
4. One deletion in every leaf: refinement approaches the raw scan, while membership lookups and certificate preparation remain sunk cost.
5. Delete a billion nonwitness edges: the answer can still use a small certificate, but reading and validating a billion change records is not free.
6. Repeated epochs: a debit reset can incorrectly revive a stale disconnected witness. An empty delta is not proof that an old certificate is intact.
7. One live source factor removed: it may delete many canonical edges. Color-failure semantics and incidence-native execution need separate mechanisms.
8. Full output every snapshot: the unavoidable vertex stream can dominate all saved edge reads.

## Changed-Design Rubber Duck

Initial design: "Store f+1 forests per block; rebuild blocks receiving more than f changes, then merge to the root."

Challenge: this merely rephrases sparse certificates and sparsification. It also expires on arbitrarily many irrelevant deletions, resets failure budgets ambiguously, and assumes that the current root must exist before a query.

Revision: charge missing stored witnesses against immutable capacities; keep complete cumulative epoch deltas; choose a cost-optimal mixed-depth cover; compute the exact current partition directly from that cover and insertions. Do not manufacture a new root artifact during every refresh.

Second challenge: selecting fewer bytes does not prove fewer I/Os if debit discovery reads everything.

Revision: make indexed witness membership an explicit charged phase, with a scan-based fallback and a separate stored-index alternative. State the cover theorem only for additive prices after that phase. Remove all unconditional speed claims.

## Evidence And Decision

The [exact commands and outputs](connectivity-paths-evidence.md) record 177,147 certificate trials: 28,624 accepts, including 3,792 above the total-deletion threshold and 8,495 changed partitions, with zero false accepts. Another 4,096 graph/edit assignments verify both the cover's additive optimum and its resulting partition. These test finite identities, not large-machine resource behavior.

The optional fragment quotient passed 4,096 exhaustive graph/edit cases with permuted external IDs and cross-shard certificates: zero partition/minimum mismatches, 3,329 cases with q<n, and 108 with q=0. A nested-cut/isolate fixture passed with five active fragments. A separate event-sweep probe checked 46,080 seven-vertex rooted-tree/cut cases and 322,560 vertex owners, with zero mismatches and an explicit nested-holes example. These tiny probes use in-memory oracle arrays; they do not validate the proposed external storage implementation.

Promote only after complete source/build/query/output/refresh trials show lower total cost than global certificates and tuned sparsification on naturally localized edits. Vary n, m, extent locality, certificate k, deletion concentration, overlap retention, slow consumers and deadlines. Measure index probes, selected bytes, DSU state misses, build traffic, output time and failure fallback.

**Hard gap:** selecting the hierarchy and k values, and paying for the global forest coordinates and membership lookup, without spending the savings in preparation or retention. The optional q-state proof removes the original 9n RAM objection, but not the full-label/minimum scan or the ET-oracle prior-art challenge. No theorem here makes real graphs locally compressible, and no experiment yet establishes a useful completion deadline on physical 4 GB hardware.

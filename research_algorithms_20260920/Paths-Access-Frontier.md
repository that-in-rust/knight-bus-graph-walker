# Paths Access Frontier

Date: 2026-09-20. Bounded follow-on research after independent review of A03. This file alone is owned by this task; the existing manuscripts are unchanged. No production implementation, physical-4-GB measurement, or global novelty claim.

## Premise Check

The [independent review, R1](Connectivity-Paths-Independent-Review.md) is decisive: the quotient in [Certified-Paths](Certified-Paths.md) is ordinary single-predecessor elimination, and source promotion is unnecessary. This note does not retest that as a contribution.

**Refined candidate:** index source-access records by tail, retain two scalar aggregates per range, and use one shared core routing tree to certify that an entire range can be replaced by one real access record. A sparse, current route-drift envelope validates these certificates after metric edits. The index may return far fewer records than the number of distinct destination gates, without enumerating per-head minima or eliminating anew for each source.

This is a narrower claim than a new path algorithm. The first idea, Euler-order colored RMQ, collapses into standard range indexing. Static cross-head pruning also overlaps transit-node post-search stalling. The remaining research question is the **constant-size range certificate plus shared drift envelope**, its paid storage, and its selective fallback under exact snapshots. The separating result below concerns source-access work against explicit competent comparators, not superiority over every possible contracted-graph index.

Expert lenses: min-plus exactness; range-query data structures; external-memory lifecycle; adversarial prior-art comparison. These are analytical perspectives, not independent endorsements.

## Prior Art Before Design

| Primary source and inspected material | Consequence |
| --- | --- |
| [Fischer, Optimal Succinctness for Range Minimum Queries](https://ae.iti.kit.edu/download/fischer10optimal.pdf), problem definition, Sections 1.1-1.2 and Table 1 | Static argmin indexing, Cartesian-tree information, and separating index space from value-array space are established. Constant-time word-RAM RMQ does not imply constant external I/O or eliminate access to a minimum's weight/provenance. |
| [Gagie, Karkkainen, Navarro, Puglisi, Colored Range Queries and Document Retrieval](https://users.dcc.uchile.cl/~gnavarro/ps/tcs12.3.pdf), Section 2's predecessor-occurrence/RMQ and rank/select framework | Listing destination colors without scanning every tail is established. Combining color listing with a per-color weight RMQ is a competent baseline. This paper's colored frequency/top-k queries are not silently treated as weighted min-per-color queries. |
| [Arz, Luxen, Sanders, Transit Node Routing Reconsidered](https://arxiv.org/pdf/1302.5611), Sections 3-4, Lemmas 1-2 and post-search-stalling inequality | An access node can already be removed when another access plus a core route is no longer. Thus cross-head seed domination and reusable access sets are not new. Its all-pairs transit table is not mandatory for every conceivable implementation; this note uses a tree of actual routes instead. |
| [Dibbelt, Strasser, Wagner, Customizable Contraction Hierarchies](https://ben-strasser.net/paper/customizable_contraction_hierarchies_arxiv_preprint.pdf), Section 2 weighted contraction, Section 7.7 partial updates, and Section 8 query scope | Witness pruning, contraction and incremental metric propagation are established. The comparator must not rebuild every shortcut unconditionally. This note's admission guard must finish before publishing an exact snapshot; delayed stale answers are not allowed. |

These are scope-checked primary sources, not claims of exhaustive literature coverage. In particular, no novelty conclusion follows from not finding this exact combination in a keyword search.

### Two Rounds Of Design

1. **Rejected as differentiation:** a source subtree becomes a tail interval; report each head's minimum through a colored index. This solves the ledger-scan problem but is standard composition.
2. **Refined once:** preserve the min-plus closure of the access set, not its entire vector of per-head minima. Test a one-landmark route cover using two interval minima and a current route-cost envelope. This can avoid reporting colors that the query does not need as independent seeds.

Do not give the second round credit for reinventing transit-node pruning. Its testable addition is a compressed way to certify many such prunings together and retain the certificate across changes to shared route costs.

## Exact Supported Contract

Use the already prepared directed retained graph F union X, finite feasible potential h, base gates P and capsule ownership pi from the existing manuscript. Reduced weights c(u,v)=w(u,v)+h(u)-h(v) are nonnegative. F is an acyclic zero-reduced-cost parent forest. Use exact integers/fixed-point integers with checked wide arithmetic and a separate infinity representation.

The paid access ledger contains each non-dominated exception and crossing F connector, including dormant quotient self-arcs, with original tail, head gate, reduced cost and stable edge identity. Let A be its record count, q=|P|, and Q the exact current gate quotient. Q can aggregate parallel gate pairs by current minimum with an original witness; source access cannot discard tails this way.

For an interior source s in capsule g:

```text
R_s = {u : pi(u)=g and s is an F ancestor of u}
access(s) = {e in ledger : tail(e) in R_s}
alpha_s(p) = min cost(e) over e in access(s) with head(e)=p.
```

Within the row for g, sort by (original_F_preorder(tail), edge_ID). The records of R_s form one contiguous range in this **filtered row**, even if other capsules create holes in global preorder. Two endpoint searches find it; no n-entry owner rewrite is required. This ordering is standard, not the contribution.

The baseline runs Q from alpha_s. For original vertex v, reduced distance is zero inside R_s and is the resulting distance to pi(v) otherwise; add h(v)-h(s) to recover original distance. For s already a gate, run the ordinary core query. The note changes only construction of the initial seeds.

**Fast snapshot profile:** h, F, pi and P remain valid and unchanged. Non-parent ledger weights/identities can change; existing-gate-headed arcs can be inserted/deleted; core weights can change. A new exception head outside P, a broken F witness, an infeasible h, a changed projection/ID interpretation, or incomplete change coverage forces ordinary exact fallback and separately admitted rebuilding. This is not a sublinear arbitrary graph-update algorithm.

## Shared Route Witness

Choose a gate z. Prepare one directed out-tree Z rooted at z using actual Q arc witnesses. It may span only a subset of gates. Shortest routes are helpful, but not required: any real rooted routes suffice.

Retain original witness identities, tree parents, Z preorder intervals and old root-to-gate path lengths L0(p). L0(z)=0. This costs O(q) words, not a q-by-q distance table. Each Q witness includes the underlying original arc and its zero-cost F prefix; retaining F unchanged makes the lift valid.

In the current snapshot let L1(p) be the length of that same route, or infinity if one of its selected original witness identities is absent. A cheaper alternative parallel arc does not silently revive a deleted selected witness. Q itself can use that alternative normally.

For a source-access cell C, store this tuple, derived from its current records:

```text
a(C), leader(C) = minimum (cost, stable_ID) among records headed at z
b(C)           = minimum [cost(e)-L0(head(e))] over records not headed at z
H(C)           = convex hull of Z preorder positions of those non-z heads
bad(C)         = number of non-z records whose heads are outside Z
count(C)       = live record count.
```

The leader header also carries its original tail and identity, so a successful query need not fetch its raw row. Empty minima are infinity. If there are only z-headed records, H is empty and b=infinity. Values in b can be negative: they are certificate values, not Dijkstra arc costs.

These tuples merge associatively using min, min/max interval endpoints and counts. No per-cell SSSP is needed. The aggregation and tree indexing are standard machinery; the graph interpretation of their predicate is the candidate.

Define a current envelope query

```text
beta(H) = max(0, max_{p with Z_preorder(p) in H} [L1(p)-L0(p)])
beta(empty) = 0.
```

A failed selected route inside H gives beta=infinity. The convex hull may include heads not actually in C: this causes conservative rejection, never an unsafe acceptance.

**Accept a cell iff** count>0, a is finite, bad=0, beta is finite, and

```text
a(C) + beta(H(C)) <= b(C).
```

All-z cells pass independently of failures elsewhere in Z. Do not accept infinity<=infinity as a valid witness test.

## Theorem: Access-Closure Substitution

For any accepted C, replacing all its records by leader(C) preserves, for every core destination y, the exact minimum cost of entering Q through C and then reaching y:

```text
min_{e in C} [cost(e) + dist_Q(head(e),y)]
    = a(C) + dist_Q(z,y).
```

Proof: for a non-z-headed e, the guard gives

```text
a + L1(head(e))
  <= a + L0(head(e)) + beta(H)
  <= L0(head(e)) + b
  <= cost(e).
```

Thus leader followed by the current Z route to head(e) is no longer than using e. Following any continuation from that head proves that the right side is no greater than each term on the left. Conversely the leader is itself one of C's real access records, so the left side is no greater than the right side. For a z-headed record its cost is at least a. Reachability/infinite distances obey the same inequalities.

If C is wholly inside the source range, s can reach its leader's original tail through F at reduced cost zero. Therefore the replacement is a real graph walk, not an invented seed. Nonnegative cycles can be removed. Q must remain an independently exact current core; the theorem does not authorize simultaneously deleting Q arcs based on circular access certificates.

Partition the source range into disjoint accepted cells and uncompressed records. Min over their closures proves exact source distances with the new seeds. No exact alpha_s vector is required. This is the distinction from per-head RMQ: **the initial vector changes, but its core-distance closure does not.**

The selected seed stores its original tail/edge. A normal settlement-ordered core predecessor forest and F paths lift the answer to a real shortest path. This note does not promise canonical ties, all shortest paths, or BFS visit order. For reached sets only, the cost inequality can be omitted if the leader and every required Z route are live; distance-mode acceptance is already sufficient.

## Query Procedure

Use a bounded balanced hierarchy over contiguous tail-ordered ledger blocks. Leaf payload is raw current ledger records, not per-source eliminated graphs. A header stores the tuple above and bounded key/child information. A partial leaf is filtered using its real tail keys.

Use one global key-ordered hierarchy across indexed capsules. A node straddling a capsule boundary cannot be accepted for a range inside one capsule, but its children can. This avoids allocating one full header/leaf per tiny capsule. A separate tree per capsule would add O(number_of_indexed_capsules) headers and needs a different storage bill.

```text
collect_source_access_frontier(s):
    pin mutually consistent F/pi, Q, access-index and route-envelope roots
    if s is a gate: run normal Q query and return
    I = locate source's interval in its capsule row
    walk index nodes intersecting I:
        if a node is fully inside I and its tuple passes the current guard:
            relax leader's head z with its cost and original witness
            do not read its ledger payload
        else if it is an internal node:
            visit intersecting children
        else:
            stream the intersecting live records as ordinary seeds
    finish exact multi-source core query
    reconstruct requested original answers and witnesses.
```

Streaming seeds can update the existing q-state solver directly. Repeated leaders at z need only one retained minimum; do not materialize all accepted records in RAM. A short target query may use the same competent CH/core oracle as the baseline, provided its multi-seed and stopping rules remain exact.

A possible additional optimization aggregates the complete range first and tries one substitution, then refines on rejection. It follows from the same associative tuple but is **not used in the retained probe**; no benefit from it is included in the reported counts.

## Snapshot Update Procedure

### Ledger Records

Recompute the tuple of each inserted/deleted/reweighted record and merge ancestors, using current reduced costs but **fixed L0**. Point changes cost O(log A) binary-header operations, plus leaf-page editing; a paged search tree can be used with explicit occupancy accounting. Tail/head moves are delete+insert. There is no need to store all old dominated records in RAM or rescan their whole capsule.

Update Q's pair minimum and original witness as part of the same snapshot. A deletion of the minimum must find the next current parallel minimum. Either charge a row scan or retain a paid (source_gate,head_gate,cost,edge_ID) index; the capacity example includes the latter. Arbitrary stable identities also need a paid lookup to their capsule/tail key.

### Core Route Drift

Keep the cumulative current differences of Z's selected edges relative to their original metric. For a live tree-edge child v, weight difference delta contributes delta to every root-path length in [tin_Z(v),tout_Z(v)). A missing selected edge contributes a failure-count interval instead. Changes to other core edges cannot invalidate Z routes, though they can change actual shortest answers.

Treat a selected witness identity whose original endpoints moved as a failed old route, unless its replacement path is explicitly reverified. An identity match alone is not proof that the same tree arc still exists. Weight drift uses the selected original record's current reduced cost, not whichever parallel record now minimizes the Q pair.

From t changed Z identities, emit start/end events, externally sort them, and sweep signed weight drift and failure count. This produces O(t+1) constant segments. Store range maxima over those segments; a failed segment has infinity. A beta query is predecessor lookup plus a range maximum. Restorations remove failure contributions; signed decreases can cancel increases on the same root path. Treating the last batch alone as the whole difference is incorrect.

This update changes the shared envelope, not every access cell mentioning a descendant head. No new shortest-path tree is required: the selected routes need remain witnesses, not remain shortest. If t fits a declared buffer this is an in-memory build; otherwise use bounded runs/merge. Rebuilding the sparse envelope per batch is the specified implementation, so the bound depends on cumulative t, not merely the latest batch size.

Complete the access, pair-minimum, Q and envelope changes before atomically publishing their roots. Pinned readers retain old versions and are charged their unique pages. If an original F edge changes, leave this fast profile; Z failures alone merely make affected certificates reject and refine to exact records.

## Independent Follow-On Review

The [shared-route review](Connectivity-Paths-Independent-Review.md#follow-on-shared-route-access-certificates) found no counterexample to the published guard under its fixed-forest, current-header and exact-core premises. The lead replayed only its new retained probe, exit 0: 22,500 source ranges, 67,500 closure checks across three variants, 9,375 drift-hull checks, and 256 factored-family source checks. These are exact small interface probes, not a full original-graph implementation or physical storage benchmark. Shared access/route identities, nested failures, restorations, head moves and cumulative signed changes were exercised.

Two independently proved optional variants are now available, without retroactively changing the original author's probe or its reported counters:

1. **Signed drift:** for nonempty H, use max over H of (L1-L0), without max(0,...); retain beta(empty)=0 and the finite/failure guards. The proof needs an upper bound, not a nonnegative one. With a=5, one other access of cost 5, old route 3 and current route 0, b=2 and beta=-3 permit exact substitution that the clipped guard rejects. This changes certificate comparisons, not the nonnegative core edge weights. The independent probe reports stricter compression in 790 ranges.
2. **Reverified fixed tree pairs:** keep Z's topology and old L0, but bind each tree pair to its current exact Q minimum and original witness. A deleted old identity need not fail the route when a different, verified original arc realizes that same pair. Build cumulative drift/absence events from changed tree pairs and update witness provenance even on an equal-weight replacement. The work parameter is changed pairs, not automatically the manuscript's t changed original identities. Exact Q formation must precede the binding. The review reports stronger compression in 2,624 ranges. This is not an implemented replacement for the author's update engine.

There are also two important limits. First, the tuple loses head/slack correlation: with old star lengths both one and current lengths two/one, cells {(z,0),(p1,2),(p2,1)} and {(z,0),(p1,1),(p2,2)} have identical a,b,hull and can share the same Q, yet only the first is exactly dominated. No guard seeing only those summaries can decide every exact substitution. Conservative rejection is necessary unless more information or work is added.

Second, a concrete factored transit-access comparator matches the stated block family, including changed metrics. Paid preprocessing verifies one offset per block and a shared head profile; one access per b_i remains valid under the shared condition max_j(w_j-j)<=2. It preserves the different x_i behavior and requires no per-source label rewrite for the tested shared metric changes. Across four metrics its 256 source checks pass; it needs 32 b-access records per metric where the author's hierarchy emits 81 records reducible to the same 32 seeds. Thus the family separates from mandatory per-head reporting, not from the strongest factored comparator. General equivalence for arbitrary valid ledgers remains unresolved. Preserve the candidate as a specific dynamic range certificate, not a new min-plus pruning principle or a proven fastest path engine.

## Separation, With A Competent Baseline

### Explicit Same-Input Family

Take a zero-weight directed chain

```text
g -> b0 -> x0 -> b1 -> x1 -> ... -> b(K-1) -> x(K-1).
```

All its vertices belong to capsule g. Other gates are z,p1,...,pk. Core edges z->pj have weight j. Let C=k+3. Add b_i->z of weight C*i and x_i->pj of weight C*i+j+2. These are ordinary explicit input arcs, not a privileged formula available only to the candidate. The probe uses C=100 for k=31.

For source b_i, the source range contains complete blocks i..K-1. Every aligned union of complete blocks has a leader at its earliest b, b(C)-a(C)=2, and a valid single-seed certificate. With k+1 and K powers of two, the ledger's binary cells align with blocks. Each source reads O(log K) headers/leader records and no raw source-access records.

**Comparators receive the identical F, pi, Q, row ordering, original IDs and exact output contract:**

- A source-seeded range scan reads (K-i)(k+1) records, not the whole unrelated capsule prefix.
- A color-listing plus per-head RMQ baseline reports k+1 exact seed minima. Give it constant time per reported color in the RAM model; do not force it to scan tails or rebuild contraction.
- The candidate reports O(log K) certificate records, reducible to one distinct gate seed. For K sources its access records are O(K log K), versus K(k+1) exact per-color seed outputs and Theta(K^2 k) scan records.

This is an **access-stage separation**, not a lower bound for shortest paths or external I/O. Full SSSP still has Omega(k) core/output work per query, and full original output has Omega(n) rows. On this family a total-runtime asymptotic gain over color seeding is not proved.

The placement of the leader matters. At source x_i, its preceding b_i is inaccessible before returning through the core. A later b has cost at least C*(i+1), greater than every direct x_i->pj cost. Hence deleting those direct accesses globally would be wrong; the last x requires all k heads as seeds. This rules out the simplistic comparator that permanently removes every covered original arc, but not a more capable source-aware contraction index.

### Exact Changed Answers Without Access-Index Rebuild

Increase the selected Z edge z->p1 by one. Every b_i's distance to p1 increases by one, but every complete-block certificate still has margin two. Only that access record's own index row, Q's weight and the sparse Z envelope need updates if all capsules are indexed; the large g source-access index is untouched. The probe indexes g and checks all K changed answers with zero raw g-ledger reads.

In contrast, explicitly storing a metric-current distance/access answer for every b_i requires updating K values. A baseline that already factors out the common core metric does not pay that cost. Thus this is not a universal dynamic-path lower bound.

### Stronger Comparators Can Match

Transit-node-style precomputed access sets can store z alone for each b_i. A range-compressed representation of those sets can match the static result. A target-specific scalar RMQ, lazy seed generation interleaved with search, CH stalling, or a suitable distance-label index can also erase a point-query advantage. Give those indexes the same preparation and retained-byte allowance.

A comparator augmented with exactly these two aggregates and the shared route envelope is algebraically the same algorithm. **No proof here separates the refined candidate from all such competent access-pruning implementations.** Its possible research value is avoiding per-source access materialization and per-cell route recustomization under one paid common witness, not a new contraction theorem.

## Failure Families And Actual Design Changes

- **No root access:** the last x in the family has k useful accesses and no z entry. The index must return k seeds. It cannot use a leader from outside the queried tail interval.
- **Sparse or poor route coverage:** a head outside Z forces refinement. A long non-shortest Z route may make b-a negative although a better core route exists.
- **Real certificate break:** ledger records z at cost 0 and p at cost 2; old Z route costs 1. Raise that route to 4. Blind reuse returns distance 4 instead of 2. The drift guard rejects and raw access restores 2.
- **Hull holes:** heads p1 and p3 need live routes, but a failed route to an unused p2 lies between them in Z preorder. The convex hull rejects unnecessarily. More head intervals could reduce rejection but cost more bytes; not added here.
- **Root missing:** deleting a selected Z edge near z can invalidate many cells. Fallback can read the complete source interval plus index overhead.
- **Frequent access edits:** maintaining both tail order and pair minima can cost more than batched sorting; cheap core drift does not make those updates free.
- **Structural fracture:** a failed F edge or new nongate exception head changes the source-region contract. The index cannot continue under old coordinates merely because its local arithmetic passes.
- **Output dominates:** a tiny access frontier does not suppress immutable vertex scans, ID joins or parent-path delivery.

The concrete revisions were: reject Euler RMQ as the alleged invention; replace full alpha with its closure; reject an unguarded static route cover after the 4-versus-2 counterexample; add a shared signed drift envelope and refine when uncertain. No second new quotient was invented.

## Bounded External Builder

1. Complete the existing source extraction, projection, normalization, h/F/pi preparation and ledger build. These are prerequisites with paid cost, not supplied for free by the index.
2. Build exact pair minima for Q with external sorting. Keep the full pair-keyed update index only if its retained allowance is available.
3. Select z and build one real out-tree Z, preferably from a paid nonnegative core SSSP. Use bounded external queue/state if q does not fit; there is no promised fast arbitrary-graph bound. Prepare Z preorder with an external stack, not a q-entry native recursion stack.
4. Join each ledger head to Z's membership, L0 and preorder. Expanded join tuples can exceed the final 24-byte ledger width; price that width. Sort by (capsule,tail_preorder,edge_ID).
5. Stream at most L records per leaf, calculate the tuple, and write headers bottom-up, one level at a time with bounded buffers. Total header count is O(A/L), not A log A. Missing storage can disable the index for selected capsules; ordinary exact source seeding remains available.
6. Build the stable-ID lookup and core pair index with separately charged sorts. Publish only after byte checks, generation consistency and complete build validation.

For sort payload X, run memory M and merge fan-in f, a declared run/merge schedule moves approximately 2X(1+ceil(log_f(ceil(X/M)))) bytes when there are multiple runs. This is implementation accounting, not a new sorting bound. Add each endpoint join, raw rewrite and output write separately. The tree header construction after sorting is linear streaming traffic; no SSSP is run per cell or per query source.

## RAM, I/O And Retention

Let V be visited access headers, U raw leaf records read, J certified leader records, and P_io the physical I/O page size. A conservative binary implementation has:

```text
access CPU = O(V log(t+2) + U + J + log A)
access I/O <= O(V + U*record_bytes/P_io + boundary_pages)
              + V * route-envelope lookup I/O + source endpoint lookups
core work = same exact Q solver/oracle as the comparator, on the new seeds
query RAM = core_solver_state(q) + bounded caches/buffers + O(log A) stack
             + optional O(t) resident drift envelope.
```

V can reach O(A_s/L+log A), and U can be Theta(A_s+L) up to the allowed occupancy/tombstone factor, for a source interval of A_s live records. The two boundary leaves may read records outside the source interval; they must not emit those records as seeds. The probe uses L=1 for logical instrumentation, not as a proposed disk block size. Packed leaf blocks such as L=128 bound decoder memory but require a workload/page-layout experiment.

A mutable paged implementation must price occupancy, splits, copy-on-write and old pinned pages, not only logical payload. Example **configured file allowances**, all decimal GB, for n=200M, m=1B, A=10M, E_Q<=10M, q=2M, t<=100,000:

| Retained object | GB allowance |
| --- | ---: |
| Raw weighted original arcs, 24m | 24.000 |
| Immutable vertex h/parent/subtree-end/original-ID rows, 24n | 4.800 |
| Dense numeric original-ID inverse lookup, 4n | 0.800 |
| Current pi plane, 4n | 0.800 |
| Tail ledger including up to 2x payload occupancy | 0.480 |
| Ledger/core row directories, about 16q | 0.032 |
| Aggregated Q including up to 2x 24E_Q payload | 0.480 |
| Stable-ID -> capsule/tail lookup including up to 2x 16A payload | 0.320 |
| Pair-keyed current-minimum update index including up to 2x 24A payload | 0.480 |
| Access headers, 128 bytes/node, at most about 4A/128 nodes | 0.040 |
| Shared Z metadata, 32q | 0.064 |
| Drift events/envelope retained allowance, 128t | 0.0128 |
| Remaining index metadata allowance | 0.250 |
| Normalized current delta allowance | 0.500 |
| Total, rounded | 33.059 |

These are admission caps, not verified implementation sizes. Reject or change the physical layout if actual capacity/fragmentation exceeds them. Arbitrary string IDs, retained parent-edge provenance and other algorithms' indexes are not included. The lead must count the entire portfolio's unique prepared union, not give this file its own 50 GB entitlement.

At q=2M, a configured 64q core state is 128 MB; a 128t envelope is at most 12.8 MB. Controlled caches, sorting buffers, runtime, decoding, source process and output buffering must still keep the worker below its provisional 3 GB allowance and the whole machine below 4 GB. Large q/t use disk-backed structures or decline the fast profile. The small probe is not that memory implementation.

The 50 GB retained limit leaves about 16.941 GB for **all** additional unique pinned prepared pages in this example. A full extra 24 GB raw generation fails. A saved u64 distance plane adds 1.6 GB; a retained full (original_ID,distance,original_parent_ID) output adds at least 4.8 GB plus statuses. Stream with bounded backpressure or admit that spool. Parent ID gathers and original edge/path unpacking add joins or potentially random I/O, not a free resident n-entry map.

```text
D_peak_host = source staging + unique pinned prepared + live build scratch
            + retained outputs + journals <= actual available storage
T_workload = extraction + base preparation + extra index construction
           + sum(access + core solve + delivered output + refresh + recovery).
```

Source scratch and merge runs need separate finite caps even though they are outside the 50 GB prepared category. A local source database must fit the same physical RAM. The index is profitable only when cumulative access savings exceed extra build/update/retention costs. No source size, measured bandwidth, useful deadline or 4 GB run was supplied, so none is claimed.

## Actual Oracle Probe

One standalone command follows. It executes the range-header procedure, point updates and a sparse signed/failure drift envelope; it is not another source-promotion/elimination-equivalence probe. Tiny in-memory helpers and recursive coordinate construction are deliberate oracle conveniences, not the external builder.

- 120 random nine-vertex capsule trees, six core-route vertices, five snapshots each; 4,800 interior-source queries.
- 1,440 ledger point updates, including deletion/restoration and head/weight changes; core-route increases, decreases and deletions.
- 12,600 route-envelope range checks against directly computed current route lengths.
- Compressed seeds compared both with full seed enumeration and with Bellman-Ford on the original retained graph, including zero edges, cycles, dormant returns and unreachable vertices.
- Explicit access-separation, changed-answer reuse, hull-hole rejection, unsafe-stale-certificate and noncompressing-source fixtures.

No B-tree split/merge, external-sort traffic, original-weight overflow boundary, original edge-path unpacking, allocator limit or device latency is tested. The query returns real current seed IDs inside the source interval. Logical header/record counts are not page-I/O measurements.

```sh
python3 - <<'PY'
from bisect import bisect_left, bisect_right
from collections import Counter
from heapq import heappush, heappop
from math import inf
from random import Random

def derive_tree_interval_coordinates(parent):
    children = [[] for _ in parent]
    for v, p in enumerate(parent):
        if p >= 0:
            children[p].append(v)
    tin, end, order = [0]*len(parent), [0]*len(parent), []
    def visit_ordered_tree_vertex(v):
        tin[v] = len(order)
        order.append(v)
        for u in children[v]:
            visit_ordered_tree_vertex(u)
        end[v] = len(order)
    for v, p in enumerate(parent):
        if p < 0:
            visit_ordered_tree_vertex(v)
    return tin, end, order

def merge_access_summary_pairs(x, y):
    return (min(x[0], y[0]), min(x[1], y[1]),
            min(x[2], y[2]), max(x[3], y[3]),
            x[4]+y[4], x[5]+y[5])

def encode_access_record_summary(record, identity, route_dist, route_tin):
    if record is None:
        return ((inf, -1), inf, len(route_dist), 0, 0, 0)
    head, cost = record
    if head == 0:
        return ((cost, identity), inf, len(route_dist), 0, 0, 1)
    if head >= len(route_dist):
        return ((inf, -1), inf, len(route_dist), 0, 1, 1)
    return ((inf, -1), cost-route_dist[head],
            route_tin[head], route_tin[head]+1, 0, 1)

def build_tail_access_index(rows, route_dist, route_tin):
    base = 1
    while base < len(rows):
        base *= 2
    empty = encode_access_record_summary(None, -1, route_dist, route_tin)
    tree = [empty]*(2*base)
    index = [list(rows), tree, base, route_dist, route_tin]
    for i, row in enumerate(rows):
        tree[base+i] = encode_access_record_summary(row, i, route_dist, route_tin)
    for i in range(base-1, 0, -1):
        tree[i] = merge_access_summary_pairs(tree[2*i], tree[2*i+1])
    return index

def update_tail_access_record(index, position, value):
    rows, tree, base, route_dist, route_tin = index
    rows[position] = value
    node = base+position
    tree[node] = encode_access_record_summary(value, position, route_dist, route_tin)
    while node > 1:
        node //= 2
        tree[node] = merge_access_summary_pairs(tree[2*node], tree[2*node+1])

def build_route_drift_envelope(parent, old_weight, current_weight):
    tin, end, order = derive_tree_interval_coordinates(parent)
    events = {}
    for v in range(1, len(parent)):
        if current_weight[v] == old_weight[v]:
            continue
        change = 0 if current_weight[v] is None else current_weight[v]-old_weight[v]
        failed = int(current_weight[v] is None)
        for coordinate, sign in ((tin[v], 1), (end[v], -1)):
            old = events.get(coordinate, (0, 0))
            events[coordinate] = (old[0]+sign*change, old[1]+sign*failed)
    bounds = sorted({0, len(parent)} | set(events))
    values, drift, failed = [], 0, 0
    for left in bounds[:-1]:
        change, failures = events.get(left, (0, 0))
        drift += change
        failed += failures
        values.append(inf if failed else max(0, drift))
    base = 1
    while base < len(values):
        base *= 2
    tree = [0]*(2*base)
    tree[base:base+len(values)] = values
    for i in range(base-1, 0, -1):
        tree[i] = max(tree[2*i], tree[2*i+1])
    return bounds, tree, base

def query_route_drift_maximum(envelope, left, right):
    if left >= right:
        return 0
    bounds, tree, base = envelope
    lo = bisect_right(bounds, left)-1+base
    hi = bisect_left(bounds, right)+base
    result = 0
    while lo < hi:
        if lo & 1:
            result = max(result, tree[lo]); lo += 1
        if hi & 1:
            hi -= 1; result = max(result, tree[hi])
        lo //= 2; hi //= 2
    return result

def collect_range_access_frontier(index, left, right, envelope, counts):
    rows, tree, base, _, _ = index
    seeds = []
    stack = [(1, 0, base)]
    while stack:
        node, lo, hi = stack.pop()
        if hi <= left or right <= lo:
            continue
        counts["headers"] += 1
        leader, residual, headlo, headhi, bad, size = tree[node]
        if not size:
            continue
        if left <= lo and hi <= right and leader[0] != inf and not bad:
            drift = query_route_drift_maximum(envelope, headlo, headhi)
            if drift != inf and leader[0]+drift <= residual:
                seeds.append(leader[1])
                counts["certified_cells"] += 1
                counts["certified_records"] += size
                continue
        if hi-lo == 1:
            seeds.append(lo)
            counts["raw_records"] += 1
        else:
            mid = (lo+hi)//2
            stack.append((2*node+1, mid, hi))
            stack.append((2*node, lo, mid))
    return seeds

def compute_core_seed_distances(size, arcs, seeds):
    adj = [[] for _ in range(size)]
    for u, v, w in arcs:
        adj[u].append((v, w))
    d = [inf]*size
    heap = []
    for v, cost in seeds:
        if cost < d[v]:
            d[v] = cost
            heappush(heap, (cost, v))
    while heap:
        cost, u = heappop(heap)
        if cost != d[u]:
            continue
        for v, w in adj[u]:
            if cost+w < d[v]:
                d[v] = cost+w
                heappush(heap, (d[v], v))
    return d

def compute_original_path_oracle(size, arcs, source):
    d = [inf]*size
    d[source] = 0
    for _ in range(size-1):
        old = list(d)
        for u, v, w in arcs:
            d[v] = min(d[v], old[u]+w)
        if d == old:
            break
    return d

rng = Random(2026092003)
counts = Counter()
for trial in range(120):
    n, q, slots = 9, 6, 3
    fparent = [-1]+[rng.randrange(v) for v in range(1, n)]
    ftin, fend, order = derive_tree_interval_coordinates(fparent)
    zparent = [-1]+[rng.randrange(v) for v in range(1, q)]
    zold = [0]+[rng.randrange(5) for _ in range(1, q)]
    zcurrent = list(zold)
    ztin, zend, _ = derive_tree_interval_coordinates(zparent)
    zdist = [0]*q
    for v in range(1, q):
        zdist[v] = zdist[zparent[v]]+zold[v]
    tails = [v for v in order for _ in range(slots)]
    keys = [ftin[v] for v in tails]
    rows = [None if rng.randrange(6) == 0 else (rng.randrange(q+1), rng.randrange(18))
            for _ in tails]
    index = build_tail_access_index(rows, zdist, ztin)
    extra = [(u, v, rng.randrange(7)) for u in range(q) for v in range(q)
             if u != v and rng.randrange(7) == 0]
    for generation in range(5):
        if generation:
            edge = rng.randrange(1, q)
            zcurrent[edge] = None if rng.randrange(4) == 0 else rng.randrange(9)
            for _ in range(3):
                position = rng.randrange(len(rows))
                value = None if rng.randrange(4) == 0 else (rng.randrange(q+1), rng.randrange(18))
                update_tail_access_record(index, position, value)
                counts["point_updates"] += 1
        envelope = build_route_drift_envelope(zparent, zold, zcurrent)
        current_zdist = [0]*q
        for v in range(1, q):
            current_zdist[v] = inf if zcurrent[v] is None else current_zdist[zparent[v]]+zcurrent[v]
        for lo in range(q):
            for hi in range(lo+1, q+1):
                expected = max(max(0, current_zdist[v]-zdist[v]) for v in range(q) if lo <= ztin[v] < hi)
                assert query_route_drift_maximum(envelope, lo, hi) == expected
                counts["drift_ranges"] += 1
        core = extra+[(zparent[v], v, zcurrent[v]) for v in range(1, q) if zcurrent[v] is not None]
        # q is the base capsule gate; raw vertex 0 is that gate.
        quotient = core+[(q, row[0], row[1]) for row in index[0] if row is not None]
        raw = [(fparent[v], v, 0) for v in range(1, n)]
        raw += [(n+u, n+v, w) for u, v, w in core]
        raw += [(tail, 0 if row[0] == q else n+row[0], row[1])
                for tail, row in zip(tails, index[0]) if row is not None]
        for source in range(1, n):
            left, right = bisect_left(keys, ftin[source]), bisect_left(keys, fend[source])
            picked = collect_range_access_frontier(index, left, right, envelope, counts)
            assert all(left <= i < right and index[0][i] is not None for i in picked)
            seeded = compute_core_seed_distances(q+1, quotient, [index[0][i] for i in picked])
            brute_seeds = [row for row in index[0][left:right] if row is not None]
            assert seeded == compute_core_seed_distances(q+1, quotient, brute_seeds)
            actual = [0 if ftin[source] <= ftin[v] < fend[source] else seeded[q] for v in range(n)]+seeded[:q]
            assert actual == compute_original_path_oracle(n+q, raw, source)
            counts["source_queries"] += 1
print("random_snapshots", dict(sorted(counts.items())), "distance_and_reachability_mismatches=0")

# Two-tail blocks: the leader is BEFORE its covered tails, so global per-tail
# edge deletion is unsound for a source at the second tail.
blocks, heads = 32, 31
parent = [-1]+[0]*heads
old = [0]+list(range(1, heads+1))
route_dist = list(old)
tin, end, _ = derive_tree_interval_coordinates(parent)
rows = []
for block in range(blocks):
    leader = 100*block
    rows.append((0, leader))
    rows += [(head, leader+head+2) for head in range(1, heads+1)]
index = build_tail_access_index(rows, route_dist, tin)
envelope = build_route_drift_envelope(parent, old, old)
family = Counter()
compressed = 0
for block in range(blocks):
    left = block*(heads+1)
    picked = collect_range_access_frontier(index, left, len(rows), envelope, family)
    compressed += len(picked)
    core = [(0, v, old[v]) for v in range(1, heads+1)]
    assert compute_core_seed_distances(heads+1, core, [rows[i] for i in picked]) == \
           compute_core_seed_distances(heads+1, core, rows[left:])
print("separating_family", "queries", blocks, "scan_records", (heads+1)*blocks*(blocks+1)//2,
      "color_minima", (heads+1)*blocks, "frontier_seeds", compressed,
      "raw_records", family["raw_records"], "headers", family["headers"])

changed = list(old)
changed[1] += 1
same_tree = index[1]
envelope = build_route_drift_envelope(parent, old, changed)
updates = Counter()
for block in range(blocks):
    left = block*(heads+1)
    picked = collect_range_access_frontier(index, left, len(rows), envelope, updates)
    core = [(0, v, changed[v]) for v in range(1, heads+1)]
    actual = compute_core_seed_distances(heads+1, core, [rows[i] for i in picked])
    assert actual == compute_core_seed_distances(heads+1, core, rows[left:])
    assert actual[1] == 100*block+2
assert index[1] is same_tree
print("shared_route_update", "changed_source_answers", blocks, "index_rebuilt=False",
      "raw_records", updates["raw_records"], "certified_cells", updates["certified_cells"])

# Head hull holes and witness failure must reject, even if the leader is live.
small = [(0, 0), (1, 2), (3, 4), (0, 5)]
p, weights = [-1, 0, 0, 0], [0, 1, 1, 3]
ztin, _, _ = derive_tree_interval_coordinates(p)
ix = build_tail_access_index(small, weights, ztin)
changed = [0, 1, None, 3]  # Unused middle head makes the hull reject conservatively.
stats = Counter()
hole_envelope = build_route_drift_envelope(p, weights, changed)
assert query_route_drift_maximum(hole_envelope, 1, 4) == inf
ids = collect_range_access_frontier(ix, 0, 4, hole_envelope, stats)
hole_core = [(0,1,1),(0,3,3)]
assert compute_core_seed_distances(4, hole_core, [small[i] for i in ids]) == \
       compute_core_seed_distances(4, hole_core, small)
print("head_hull_hole", "whole_cell_rejected=True", "exact_fallback=True")
env = build_route_drift_envelope(p, weights, [0, 4, 1, 3])
stats = Counter()
ids = collect_range_access_frontier(ix, 0, 2, env, stats)
assert compute_core_seed_distances(4, [(0,1,4),(0,2,1),(0,3,3)], [small[i] for i in ids])[1] == 2
assert compute_core_seed_distances(4, [(0,1,4),(0,2,1),(0,3,3)], [small[0]])[1] == 4
print("unsafe_stale_certificate", "guarded_distance=2", "unguarded_distance=4")

point = (blocks-1)*(heads+1)+1
env = build_route_drift_envelope(parent, old, old)
ids = collect_range_access_frontier(index, point, len(rows), env, Counter())
assert len(ids) == heads
print("failure_family", "last_second_tail_seeds", len(ids), "no_access_compression=confirmed")

PY
```

Observed stdout, exit status 0:

```text
random_snapshots {'certified_cells': 3338, 'certified_records': 4105, 'drift_ranges': 12600, 'headers': 72658, 'point_updates': 1440, 'raw_records': 20171, 'source_queries': 4800} distance_and_reachability_mismatches=0
separating_family queries 32 scan_records 16896 color_minima 1024 frontier_seeds 81 raw_records 0 headers 210
shared_route_update changed_source_answers 32 index_rebuilt=False raw_records 0 certified_cells 81
head_hull_hole whole_cell_rejected=True exact_fallback=True
unsafe_stale_certificate guarded_distance=2 unguarded_distance=4
failure_family last_second_tail_seeds 31 no_access_compression=confirmed
```

The 81 frontier seeds are certificate records read across 32 queries; since they all head at z, ordinary minimum reduction leaves one distinct gate seed per query. This is not a claim of 81 independent shortest-path states.

## Final Verdict

**What survived:** an exact, implementable access-stage mechanism that substitutes a range using two scalar minima, one real leader, a head hull and a shared current route envelope. It avoids raw source-range scans on a specified family, can use fewer seeds than distinct heads, and survives some metric changes that genuinely change answers. The probe exercised those procedures and rejection paths.

**What did not survive as novelty:** source elimination, Euler range conversion, RMQ, cross-head route domination, static access-node pruning, min/max aggregation or range-add/range-max drift maintenance. Prior art and the constructive reductions occupy all of them separately; a static transit-access representation can match the separating family.

**Research status:** differentiated from the existing ledger-scan A03 implementation and the stated per-head-seeding baseline, but **not established as a new algorithmic contribution against the strongest access-pruning baseline**. The one-level refinement is the shared-witness aggregate certificate and its metric-validity contract. A broader precedent search or an equally compressed TNR/CH implementation could still subsume it.

The decisive hard gap is not correctness of the inequality. It is whether real source ranges contain affordable landmark leaders with enough route slack, while route hulls stay selective and index maintenance/output costs remain worthwhile. No further name or broader novelty claim is warranted without that evidence.

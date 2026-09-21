# Paths Correlated Guard Study

Date: 2026-09-20. Independent, bounded A03 research sidecar. Only this new file is owned by this task. No production code, commits, previous-probe replay, embedding work, or claim of algorithmic novelty.

## Finding First

**Yes: retaining head/slack correlation gives a strictly stronger sound skipping test at explicit, paid bounds, including under evolving route labels. No: the exact construction below is not a differentiated algorithm. It is dynamic orthogonal range minimum searching composed with a piecewise-constant label query.**

There is a useful intermediate choice: a fixed number of head bins per access header strictly strengthens the signed-extrema guard at a fixed storage multiplier. It is ordinary partitioned range aggregation, not a new pruning principle. For an exact summary, even one changed route edge can interrogate any head's slack; a self-contained constant-word summary cannot answer every such future query. A convex hull over numerical head IDs is unsound; a hull over a genuinely one-parameter metric is valid but is precisely a standard lower-envelope query.

**Disposition:** retain the bounded-bin guard as a possible engineering experiment. Do not promote either it or the exact range-index construction as the missing genuine A03 innovation. A novelty hypothesis would require an additional restriction or a better storage/update/query tradeoff against the matched indexes below.

## Premise And Scope

Read in this session:

- [Paths-Access-Frontier.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Paths-Access-Frontier.md), including its shared-route predicate, update profile, storage bill and incorporated independent findings.
- [Certified-Paths.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Certified-Paths.md), including the source-seeded formulation and specialized-elimination boundary.
- [Connectivity-Paths-Independent-Review.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Paths-Independent-Review.md), R1 and AF1-AF4.

R1's elimination equivalence, AF2's signed drift and verified tree-pair alternatives, and AF4's factored comparator are settled premises here, not experiments to repeat. AF3 already proves that the existing tuple loses correlation. The question is what extra information costs and whether the resulting operation escapes established range searching.

The analysis separates three issues: graph soundness, data-structure cost, and novelty against equal information. These are analytical checks by one researcher, not a simulated independent panel.

The original fixed-snapshot contract remains mandatory: feasible potential h; intact zero-reduced-cost forest F; fixed capsule ownership pi and gates P; complete current access records; independently exact core Q; mutually consistent published roots; checked exact arithmetic and original witness identities. Structural fracture or a new head outside P leaves this fast profile. No seed-compression decision deletes an arc from Q.

Here, "labels evolve" means costs of the same verified route-tree topology evolve, including failures/restorations. Access costs may also change through explicitly paid index updates. It does not mean arbitrary changes to h, F, pi, head numbering or the route-tree topology.

## Operator And Domain

Let I be a source-access interval in the existing capsule/tail/edge-ID order, and C its live records. Every record has a tail key x(e), head p(e), reduced cost c(e), and stable original witness. Let z be the fixed route root and a(C) the minimum cost of a z-headed record in C, with its actual leader witness.

For a non-z head covered by the fixed route tree Z, define

```text
y(p)       = fixed Z preorder coordinate
L0(p)      = original length of the selected/reverified tree route
L(p)       = current length of that route, or FAILED
delta(p)   = L(p) - L0(p), when the route is live
sigma(e)   = c(e) - L0(p(e))
m_C(p)     = min sigma(e) for records in C headed at p
mu_C(delta)= min over occupied non-z heads p of [m_C(p) - delta(p)].
```

Empty head minima are omitted. Any occupied uncovered/failed head prevents this route certificate. Empty C emits nothing; an all-z C needs only a finite current leader. Never evaluate infinity minus infinity, or accept infinity <= infinity as a witness.

**Correlated route guard:** accept C exactly when a current leader exists, every required route is live and covered, and a(C) <= mu_C(delta).

This decides *domination by these selected routes*. It is sufficient, not generally necessary, for exact access-closure substitution when Z routes are longer than shortest paths.

For each access e, acceptance is precisely

```text
a + L(p(e)) <= c(e).
```

Following that real route and then any Q continuation proves

```text
min_{e in C} [c(e) + dist_Q(p(e), v)]
    = a + dist_Q(z, v)       for every core destination v.
```

The reverse inequality follows because the leader is itself in C. Its tail is reachable from the actual source through F. This proves sound skipping and preserves witness reconstruction under the existing contract.

For completeness, replacing L(p) with actual dist_Q(z,p) makes the inequalities necessary too: evaluate closure equality at v=p(e), where the original access set can reach p(e) for at most c(e). Therefore failures of a nonshortest Z test are not proofs that seed substitution itself is impossible. In the star constructions below, L equals the exact core distance, so the distinction disappears.

## Bounded Correlation Candidate

### Stored Summary And Procedure

Fix B nonoverlapping, contiguous bins of Z preorder, shared by every access cell. Boundaries are chosen and paid during preparation, not moved to suit an individual query. Use nested partitions when comparing B=1,2,4,...

Replace the single nonleader summary in a cell C with, for each bin j:

```text
n_j(C) = number of live non-z records in that bin
b_j(C) = min sigma(e) among those records
H_j(C) = hull of their actual head coordinates, empty if n_j=0.
```

Keep the original a/leader, total live count, and outside-Z count. Per-bin tuples merge by count addition, minimum and hull union, so header construction is associative. This preserves coarse correlation between head region and access slack; it is not a full joint distribution.

For each nonempty bin obtain the signed current drift maximum beta_j over H_j. Reject if it includes a failed route. Accept precisely when the common coverage/leader tests hold and

```text
a + beta_j <= b_j       for every nonempty bin j.
```

Use the existing tail hierarchy: accept only cells fully inside the source interval; otherwise descend and ultimately stream filtered raw records. A bin never licenses a leader outside the queried range. Alternatively, aggregate the entire source range before its first test; grant that same optimization to every comparator.

### Strength And Limits

Let b be the old global minimum and beta the signed maximum over the old global hull. For every nonempty bin, b_j >= b and beta_j <= beta. Thus every old acceptance is a new acceptance. Splitting a bin again cannot invalidate an acceptance. With singleton-head bins, the test equals the correlated route guard.

A strict example without hull holes, failed routes or signed-decrease effects:

```text
heads                 p0  p1  p2  p3
L0                     3   3   3   3
current delta          0   0   3   3
current access cost    3   3   6   6
sigma                  0   0   3   3
leader: (z,0)
bins: {p0,p1}, {p2,p3}.
```

The single-bin test rejects: 0+3 > 0. Both two-bin inequalities hold with equality. On a core star z->pi these are actual routes and exact distances. Starting from delta=0 and increasing the last two star edges changes answers while leaving the access summaries unchanged.

All four heads are present, so this is specifically restored slack/head correlation. It is not merely avoiding an unused failed head. A two-bin failure remains possible inside either bin: pair a small slack with a small drift and a large slack with a large drift in the same bin. More bins spend space to resolve more such pairings.

### Concrete Bounds

Let A be live/reserved ledger slots under a fixed occupancy bound, L the maximum records per packed leaf, and N=Theta(1+A/L) the leaf count. The following simple profile fixes the tail-slot layout for an epoch; it permits cost/head changes among existing gates, deletion, and restoration. New tail keys requiring reblocking incur a separately paid reblock/rebuild, not an unproved worst-case update guarantee.

- Extra header storage: O(BN) words, versus O(N) for separate extrema.
- Streaming preparation after the existing head join and tail sort: O(A+BN) CPU/words written.
- One slot edit: rescan its O(L) leaf, rebuild its B entries, and merge ancestors in O(L+B log(N+1)) CPU. Signed slack uses the new cost and fixed L0.
- One tested header: O(B log(r+1)) CPU using the shared r-segment drift index; no raw access payload is required on success.
- V visited headers and U raw records: O(V B log(r+1)+U+log(A+1)) access CPU, plus interval-location work.
- Working state: bounded leaf/cache buffers, O(B+log(N+1)) traversal/merge state, plus the same core solver. The headers can be disk resident.

A possible packed bin payload is 32 bytes: signed 128-bit minimum, two u32 hull endpoints, count/flags and padding. Replacing one bin with B bins adds 32(B-1) bytes per header before final layout alignment. At A=10 million, L=128, and at most 4A/L headers, B=4 adds 30 MB logical payload; B=8 adds 70 MB. These are arithmetic layout examples, not measured allocations. Page rounding, sparse-capsule layout, updates, old pinned pages and rebuild coexistence still count.

The bound is defensible, but the operation is exactly the same as conventional fixed head partitioning with per-partition range minima. No matched baseline must continue using only one global minimum after receiving these extra summaries.

## Exact Correlation Is Orthogonal Range Minimum

### Reduction

Represent each covered non-z access as the weighted point

```text
(x(e), y(p(e))), weight sigma(e).
R(I,J) = min sigma(e) with x(e) in I and y(p(e)) in J.
```

Use a separate ordinary tail-key index to query a/leader, live/outside-Z counts, and the nonleader head hull. Let the shared current route envelope be partitioned into disjoint half-open head intervals J_1,...,J_r, with constant signed drift d_j or FAILED on each. Then

```text
mu_C(delta) = min over j with R(I,J_j) finite of [R(I,J_j) - d_j],
```

provided every occupied J_j is live. A failed segment with no access in I is irrelevant.

Proof: the rectangles I x J_j partition the nonleader records, and d_j is constant within each part; minimum commutes with subtracting that constant. Hence this returns the exact joint minimum, not the minimum slack minus an unrelated maximum drift. This equality handles signed changes, coincident head coordinates, parallel accesses and empty head gaps.

Operationally:

```text
check_correlated_access_guard(I):
    pin consistent access/core/route versions
    obtain current a, actual leader, count, and outside-Z count in I
    return EMPTY if count is zero
    reject if leader is absent or outside-Z count is positive
    for each current envelope segment J intersecting I's head hull:
        s = R(I, J)
        if s is EMPTY: continue
        if J is FAILED: reject
        if a + drift(J) > s: reject
    accept and emit the actual leader
```

A failed test means refine or seed ordinarily, not "distance changed" or "no path." Early rejection is legitimate. Scanning all relevant envelope segments gives the following bound even when most are empty.

### Fully Specified Matched Implementation

A conservative exact implementation needs no new data structure:

1. Build a fixed balanced binary segment tree over the q route-head coordinates, with a power-of-two padded universe. It has O(q) nodes and height h=ceil(log2(max(2,q))).
2. At each head-tree node store an augmented balanced search tree of the access points below it, ordered by the complete tail key (capsule, tail preorder, stable ID). Its aggregate is the minimum signed slack; a range query returns EMPTY or that minimum.
3. Store each access in the h+1 head catalogs on its root-to-leaf path. Duplicates at one head retain separate tail keys, so deleting a current minimum exposes the next actual record.
4. Decompose J into O(h) canonical head nodes. Query I in each corresponding tail tree and take the minimum.
5. A cost update changes the point's weight in its catalogs. A head/tail move is deletion plus insertion. New keys do not require changing existing tail ranks because comparisons use the actual immutable key tuple.

With a standard deterministic balanced tail tree, one rectangle or access edit takes O(h log(A+1)) CPU; storage is O(Ah+q) words. An explicit bulk build sorts A points by tail key, distributes them into head catalogs preserving that order, and builds balanced tail trees bottom-up. CPU is O(A log(A+1)+Ah+q), and the expanded catalog output is O(Ah+q). A conservative external builder sorts the O(Ah) expanded records by (catalog,tail key), then streams balanced-tree construction with bounded buffers. Charge that sort, its expanded tuple width, temporary runs and output; it is not an O(A)-byte build.

If r_I envelope segments intersect the source's head hull, a complete exact guard costs

```text
O(log(A+1) + log(r+1) + r_I h log(A+1)).
```

An all-z range bypasses this work. The I-only header lookup and segment iteration are included. This exact test can run on a whole source interval, so it need not repeat a full r_I scan at every rejected hierarchy level. If a caller does repeat it, sum r_I over all calls; there is no hidden one-scan guarantee.

The method stores all head/slack information and evaluates the same predicate as a conventional weighted range tree. Label evolution does not make the geometric points move: only access-cost/head/tail edits move or reweight them. This separation is important for the stronger static comparator below.

### Label Evolution And RAM Accounting

For t cumulatively changed selected tree identities, or t changed verified tree pairs under AF2's separate binding contract, each route-edge change contributes a subtree interval. A current event sweep gives at most 2t+1 elementary head intervals, coalescing adjacent equal drift/failure states. Thus r=O(t+1), not O(latest_batch_size). Numerical drift and failure multiplicities are maintained separately; restoring one ancestor does not clear a failed descendant.

Rebuilding this shared envelope costs O(t log(t+1)) CPU and O(t+1) space, or the paid external sort/sweep. Core changes do not rewrite the access catalogs. Access edits still update their own points; a record also used by Q/Z must be handled on both sides before publishing. Replacing the topology or L0 requires rejoining/rebuilding the index.

These are word-RAM bounds assuming keys/slacks/route arithmetic occupy O(1) checked words. They do not prove physical 4 GB performance. A bounded-cache disk representation can use O(log q+log A) traversal state; conservatively each visited catalog node can incur a page transfer. No RAM logarithm is silently divided by page size.

The exact index can be much more expensive than B-bin headers. Illustratively, q=2 million gives h=21; A=10 million can yield 220 million catalog occurrences. At an illustrative 64 bytes per augmented occurrence, that is 14.08 GB before outer directories, occupancy and pinned copies. It cannot be resident on a 4 GB machine. The author's roughly 33.059 GB base allowance plus this illustrative payload is already 47.139 GB of the *shared* 50 GB prepared-union limit. Packed static layouts may use less; mutable layouts may use more. Do not reserve this space independently of other families.

Raw ledger/Q retention, preprocessing SSSP, all source metadata, reconstruction, complete output, source staging, scratch and recovery remain the original paid obligations. Only source-access guard work is being changed.

## Why A Constant Exact Summary Fails

This strengthens AF3's two-instance obstruction into an information bound; it is an elementary derivation here, not a claimed new lower-bound result in the literature.

Fix a single cell with leader cost a=0 and k distinct non-z heads. Each old star edge z->p_i has length 1. Access cost at p_i is 1+s_i, with independently chosen integer s_i in {0,...,U}. At the base metric every access is dominated.

Now issue any one-edge label query: increase just z->p_j by d in {1,...,U}, leaving all other route labels unchanged. The exact answer to the domination test is

```text
ACCEPT iff s_j >= d.
```

Every pair of different slack vectors is separated by one such query: choose a coordinate where they differ and a threshold between those two integers. Therefore a deterministic self-contained summary that answers all future one-edge tests must distinguish (U+1)^k possibilities and needs at least k log2(U+1) bits in the worst case. This obstruction holds even though t=1 in each test.

It can be realized with an identical exact Q across the different slack vectors: place cheaper g->z and g->p_i accesses at tails outside the queried subtree in capsule g, fixing those Q pair minima; keep g unreachable from z. The query's accesses still differ, while z reaches p_i only along the star. Thus Q is not a hidden encoding of the discarded slacks.

Scope matters:

- This is a summary-information bound, not a query-time or cell-probe lower bound.
- It excludes no stronger sufficient-only O(1)-word guard; B=2 already gives one.
- It allows compression of structured profiles and sharing between overlapping cells.
- Reading the raw/indexed slacks during label preparation is permitted but is additional work, not a way to keep both the summary and preparation free.
- It does not prove that every query reads k heads. The exact range index can read far fewer.
- The bit bound assumes a bounded word model, not an unbounded integer packing an entire vector into one unit-cost word.

Hence "keep the currently critical head(s), and trust that future small updates preserve them" is not exact in the general admitted label domain. Any head can become decisive after one route edit.

## Hull And Kinetic Temptations

**Wrong coordinates:** a convex hull of (head preorder, slack) does not preserve minima under arbitrary head-dependent drift. Consider points (0,0), (1,1), (2,0). The middle point is absent from the lower hull. With drift (0,2,0), it alone violates the a=0 test. Evaluating only lower-hull vertices accepts incorrectly. Head preorder is an ordering, not an assumption of affine route drift.

**Valid but known restriction:** suppose a declared one-parameter metric gives delta(p)=theta*f(p), with fixed f and static access slacks. Then

```text
mu_C(theta) = min_e [sigma(e) - theta*f(p(e))].
```

Each record is a line in theta. The necessary summary is its lower envelope, equivalently a planar hull in dual coordinates. For k relevant lines, sorting/building an envelope costs O(k log k), worst-case envelope size is O(k), and a query can binary-search ordered breakpoints in O(log k). These elementary bounds do not include tail-range indexing or rebuilding after access edits.

Calling the winning line a "critical access certificate" does not change this operation. If coefficients themselves follow known low-complexity time trajectories and events are scheduled, it becomes a kinetic envelope problem. The kinetic paper read below explicitly works with such trajectories, failure certificates and event accounting. It does not license arbitrary unscheduled graph edits at a constant amortized cost.

For independent route-edge changes, delta(p) is a dot product between edge-drift values and a head's ancestor-incidence vector. That is a multi-parameter lower-envelope problem, not automatically a planar hull. In a star the vectors are coordinate basis vectors, so every head can become the unique minimizer. Laminar subtree updates instead yield the interval decomposition above; they do not supply a one-dimensional affine metric parameter.

## Matched Competent Baselines

All comparisons receive the same current raw ledger, tail/head coordinates, h/F/pi/Q/Z, complete edit receipts, precision, witnesses, output contract and total storage allowance. Extra prepared information must be available to both sides and paid once on each side. No baseline is forced to report an exact alpha vector when closure-preserving seeds suffice.

| Comparator | Same-information consequence |
| --- | --- |
| Signed global extrema | Correct low-space control. The two-bin example proves strictly stronger acceptance, not better runtime. |
| Conventional B-partition range aggregation | Stores exactly b_j/H_j/count and evaluates exactly the same inequalities. It matches the bounded candidate's bound and accepts precisely the same cells. |
| Dynamic orthogonal weighted range tree | Stores the same points and evaluates the same segment rectangles. It is the exact construction above, not a weaker competing algorithm. |
| Static orthogonal range-minimum index | When only route labels change, points and priorities stay fixed. Nekrich's primary result gives O(A) words with O(log^epsilon A) per rectangle for fixed epsilon>0, or O(A log log A) words with O(log log A) queries. This is a stronger RAM comparator on those epochs. Pay its construction P_ORM(A); this note does not establish an implementation/build-time or external-memory bound for it. Access edits require rebuild or a separately proved dynamic layer. |
| Per-cell current head minima | For a hot fixed cell, retain its per-head slack minima in a head-ordered lazy tree. A route subtree edit is a head-interval add to current slack-minus-drift values; track route-failure counts separately. Updating that cell costs O(log(k_C+1)); its root gives the current minimum. Maintaining all such cells pays affected-cell fanout and retained catalogs. High repeated-query locality may favor this over scanning the shared envelope each time. |
| Factored transit access | If paid verification finds shared head-cost profiles plus cell offsets, reuse them and test the shared profile once. AF4 already matches the old family. The new four-head example is not offered as a separation from this comparator. |

For dynamic weighted range searching, [Lueker's primary report](https://escholarship.org/content/qt9k40c7jc/qt9k40c7jc_noSplash_842cfde126b8a61d147f02f6ddd08bd4.pdf) explicitly admits min as an aggregate, uses auxiliary trees on coordinates, and supports deletions. The identification is constructive, not just an analogy. The sharper static bounds above are from [Nekrich, Section 4 and Appendix D](https://arxiv.org/pdf/2007.11094), not a claim that the simple dynamic construction attains them.

The static theorem uses rank-space coordinates and logarithmic-size words. Keep original slacks and witnesses alongside the index; returned priorities are not substitutes for numerical slack. Break coordinate ties with stable IDs while mapping a head boundary to the entire tied group. A conservative comparison-based conversion costs O(log(A+1)) per endpoint. In an access-static epoch, head boundaries can be converted once during each envelope rebuild in O(r log(A+1)) time and O(r) stored ranks; source-tail endpoints still cost O(log(A+1)) per query. Otherwise add head-rank conversion to each rectangle query. These costs and P_ORM(A) are not hidden inside the quoted static theorem.

The current-per-cell comparator pays one interval update per affected materialized cell, even if many cells share a head. Access edits also require maintaining the actual per-head access minimum, for example with paid per-head multisets, and updating the corresponding head-tree leaf in each affected cell. A lazy on-demand route version can replay its missing edit log instead; replay and log retention are charged. Neither comparator must rescan all its access records after every route change.

## Primary Sources Actually Read

URLs below were opened and the stated text inspected during this task. Scope is intentionally narrow.

| Source | Inspected material and use |
| --- | --- |
| [George S. Lueker, A Data Structure for Dynamic Range Queries, 1978 report](https://escholarship.org/content/qt9k40c7jc/qt9k40c7jc_noSplash_842cfde126b8a61d147f02f6ddd08bd4.pdf) | Printed pp. 3, 5-8 and the insertion/rebalancing discussion: admissible min aggregation, coordinate decomposition and auxiliary range trees. Establishes the nearest dynamic range-query machinery, not a graph-specific claim. |
| [Yakov Nekrich, New Data Structures for Orthogonal Range Reporting and Range Minima Queries](https://arxiv.org/pdf/2007.11094) | Introduction/Table 1, Section 4's two-dimensional minima paragraph, Appendix D/Theorem 5. Establishes the static weighted 2D comparator. Its reporting results are not substituted for minima, and static bounds are not transferred to point updates. |
| [Alexandron, Kaplan, Sharir, Kinetic and Dynamic Data Structures for Convex Hulls and Upper Envelopes](https://www.cs.tau.ac.il/~haimk/papers/cgta.pdf) | Introduction's trajectory/event assumptions, Proposition 1.1 and start of Section 2. Establishes the envelope/hull correspondence and why kinetic maintenance needs a motion/event model. No event bound is claimed for arbitrary graph receipts. |
| [Arz, Luxen, Sanders, Transit Node Routing Reconsidered](https://arxiv.org/pdf/1302.5611) | Section 4's post-search-stalling inequality and Lemma 2; Section 7's access-set overlap compression discussion. The graph substitution principle and compressed-access baseline are established; this is not evidence that the paper implements these particular correlated summaries. |

The attempted Fischer PDF endpoint returned a fetch error; it is not counted as a source read here. No search-result snippet alone supports a theorem in this study. This is not an exhaustive priority search, and absence of a matching keyword is not novelty evidence.

## New Bounded Verification

Only the new algebraic probe below was run. It does not import any author implementation or execute any existing probe. It checks the previously untested partition-strength hierarchy and exact drift-segment join. It is not a balanced-tree implementation, graph-solver benchmark, physical-memory run, or proof of the storage constants.

Domain: four occupied heads, each slack in 0..3; each drift in {-1,0,1,3,FAILED}; leader cost 0 or 1. These signed finite values are realizable with old star lengths 3 and nonnegative current lengths. All heads are occupied, deliberately excluding hull-hole gains from the main count. Cases are combinations, not distinct production graphs.

Observed exit status: 0.

Lead integration check: the retained new probe was inspected and replayed once, with the same exit status and counts below. This is a replay of the author's algebraic probe, not a second independent algorithm implementation. The lead also checked the static range-minima comparator directly in Nekrich's primary text (introduction, section 4 and Appendix D).

```text
correlation_audit {'cases': 320000, 'two_over_one': 5378, 'exact_over_two': 9872} mismatches=0
single_edge_signatures 256 of 256
head_rank_hull_mutation unsafe_acceptance_detected
access_weight_then_label_edit reject_then_restore
```

The signature check distinguishes all 4^4 slack vectors using only one-edge label queries. The hull mutation demonstrates an unsafe proposed compression, not a failure of either sound guard. The final small sequence checks that current access slack changes can break the test before a subsequent route-label decrease restores it.

Reproduction from the repo root, restricted to this new code block:

```sh
awk '/^<!-- CORRELATED-GUARD-PROBE-START -->/{p=1;next} /^<!-- CORRELATED-GUARD-PROBE-END -->/{p=0} p && !/^```/' research_algorithms_20260920/Paths-Correlated-Guard-Study.md | python3 -
```

<!-- CORRELATED-GUARD-PROBE-START -->
```python
from itertools import groupby, product

def accept_partition_guard_exact(slacks, drift, leader, width):
    for left in range(0, len(slacks), width):
        right = min(left + width, len(slacks))
        values = drift[left:right]
        if None in values:
            return False
        if leader + max(values) > min(slacks[left:right]):
            return False
    return True

def accept_segment_join_guard(slacks, drift, leader):
    offset = 0
    for value, run in groupby(drift):
        size = len(list(run))
        if value is None or leader + value > min(slacks[offset:offset + size]):
            return False
        offset += size
    return True

counts = dict(cases=0, two_over_one=0, exact_over_two=0)
for slacks in product(range(4), repeat=4):
    for drift in product((-1, 0, 1, 3, None), repeat=4):
        for leader in (0, 1):
            oracle = all(d is not None and leader + d <= s
                         for s, d in zip(slacks, drift))
            one = accept_partition_guard_exact(slacks, drift, leader, 4)
            two = accept_partition_guard_exact(slacks, drift, leader, 2)
            four = accept_partition_guard_exact(slacks, drift, leader, 1)
            joined = accept_segment_join_guard(slacks, drift, leader)
            assert not one or two
            assert not two or four
            assert four == joined == oracle
            counts["cases"] += 1
            counts["two_over_one"] += two and not one
            counts["exact_over_two"] += oracle and not two
print("correlation_audit", counts, "mismatches=0")

signatures = set()
for slacks in product(range(4), repeat=4):
    bits = []
    for head in range(4):
        for increase in range(1, 4):
            drift = [0] * 4
            drift[head] = increase
            bits.append(accept_segment_join_guard(slacks, drift, 0))
    signatures.add(tuple(bits))
assert len(signatures) == 4 ** 4
print("single_edge_signatures", len(signatures), "of", 4 ** 4)

# A lower hull of (head rank, slack) drops the interior point (1,1).
slacks, drift = (0, 1, 0), (0, 2, 0)
hull_only = all(drift[i] <= slacks[i] for i in (0, 2))
assert hull_only and not accept_segment_join_guard(slacks, drift, 0)
print("head_rank_hull_mutation unsafe_acceptance_detected")

# Reweighting the access record can revive a previously irrelevant slack.
assert accept_segment_join_guard((3, 0, 3, 0), (3, 0, 3, 0), 0)
assert not accept_segment_join_guard((2, 0, 3, 0), (3, 0, 3, 0), 0)
assert accept_segment_join_guard((2, 0, 3, 0), (2, 0, 3, 0), 0)
print("access_weight_then_label_edit reject_then_restore")

```
<!-- CORRELATED-GUARD-PROBE-END -->

## Falsifiable Next Experiment

**One bounded question:** on an unfactored changing ledger, does a small B-bin upgrade beat signed extrema on full lifecycle cost under a common byte cap? Include a conventional index given exactly the B-bin summaries as the equal-information control. It must match the predicate; any unexplained acceptance advantage is a bug or an information mismatch. Separately measure whether a full exact range index amortizes its larger preparation/storage bill.

Use one new seeded ledger workload, not the settled promotion, AF2 or AF4 fixtures:

1. Fix valid h/F/pi/Z and a tail hierarchy. Use many irregular per-head slack profiles across cells, with duplicate heads and no privileged formulas. Verify and report any discovered shared profiles; give them to the factored baseline.
2. Cross B in {1,2,4,8}, cumulative drift fragmentation r in {1,8,64,512}, and cold-source versus hot-source reuse. Include signed route changes and access-cost edits. One control repeatedly edits the same route edge so history length grows while cumulative t stays one.
3. Compare signed extrema; B-bin candidate; identical conventional B-bin range aggregation; exact dynamic range-tree/segment join; and per-cell materialized minima. On access-static epochs also include a competent static rectangle-minimum implementation. Use the same total byte cap and bounded cache; do not require the exact index to fit at a cap where it does not.
4. Report build/refresh time, actual retained and pinned bytes, header/catalog/page visits, envelope segments examined, raw payload avoided, accept/reject counts, and source-to-answer time including the unchanged core/output work. Use a small direct exact guard only as the correctness oracle, never as the competitive performance baseline.
5. Include the adversary where low slack and high drift alternate inside every fixed bin, then permute heads so the partition is unhelpful. Include label fragmentation large enough that segment enumeration loses to a current-per-cell minimum.

Predeclare a finite query count Q and measure the workload inequality

```text
extra_build + extra_refresh
    < sum_{i=1..Q}(baseline_total_query_i - candidate_total_query_i),
```

subject to the same peak-RAM, prepared-union, scratch and output caps. Failure of that inequality on the intended workload rejects the engineering hypothesis. Equality of the B-bin implementations confirms the reduction, not an innovation. A claim of genuine differentiation would need a newly specified restriction and a measured or proved advantage over these equal-information comparators; none is supplied here.

## Provenance And Write Boundary

Read-only input hashes at inspection:

```text
863fef7644c4d7845e621dec4f013fc0566b957221589b101d7d171193357013  Paths-Access-Frontier.md
5ae50bfb3fe20340f5479d6b5b1f3423512931da41d9029d199378dc6de44a29  Certified-Paths.md
7bf6fbc0f66070f80384c80ec8d6be16254512424c2cc41385b3b0077177e82c  Connectivity-Paths-Independent-Review.md
```

Only research_algorithms_20260920/Paths-Correlated-Guard-Study.md was created by this task. Existing manuscripts, independent reviews, shared journals, production files and commits were not changed by this task. Other active work may independently advance shared files.

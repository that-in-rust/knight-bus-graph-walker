# Connectivity Fault-Cover: Independent Review

Review date: 2026-09-21. Scope: the proposed native overlapping-clique fault-cover WCC kernel, not the lead implementation. This review owns only this file. No lead code is imported, and no commits or other file edits are part of the review.

## Premise Check

**Verdict: the specified connectivity reduction and complete minimum-original-ID output are sound under the normalized simple-graph contract.** The `O(F+k)` claim is a query-working-state claim in words, conditional on paid, replayable external views and bounded buffers. It is not total storage, total work, a `k`-only parameterized kernel, or a proved advantage over all forest methods.

This revision incorporates the lead's sharper **pigeonhole attachment plus blocked-only core enumeration**. The initial all-core-postings `C=sum r_f^2`, unconditional deletion join, and `k^2` flag scans are historical draft costs, not the current recommendation. The sharper construction preserves the partition of the full annotated torso without explicitly realizing every torso edge.

**Most important challenge:** the forest countercontrol extends beyond the requested single-clique path. An immutable DFS forest of any union of `F` cliques has at most `F` rooted leaves in nontrivial trees. Consequently, deletions covered by `S` cut at most `2k+F` of its tree edges. With the same elimination of incidence-free insertion endpoints, a forest-fragment baseline can also have `O(F+k)` mutable component state. Proof below. The remaining plausible distinction is preparation/retained bytes, incidence-native execution, and measured traffic, not an asymptotic mutable-state separation.

Expert lenses used: exact quotient correctness; external-memory accounting; adversarial finite verification; primary-literature overlap. These are review perspectives, not a claim that other agents ran.

Read-only local context: [Witness-Connectivity.md](Witness-Connectivity.md), including its global forest-fragment bound and full-output obligations; D06 and its structural-admission caveats in [Architecture-Decision-Map.md](../research_4gb_20260919/Architecture-Decision-Map.md); the factor-aware baseline requirement in [Final-Research-Decision-Brief.md](../research_4gb_20260919/Final-Research-Decision-Brief.md); and the mathematical contract/refinements in the lead's [Connectivity-Fault-Cover-Quotient.md](Connectivity-Fault-Cover-Quotient.md). No lead implementation or test code was read or run.

## Exact Contract

Let `V` be a finite, fixed, totally ordered set of original IDs, including isolates. Every `S_f` is a set of vertices, with duplicate membership records removed. Distinct factor identities may have identical sets. Empty and singleton factors are harmless. Let

```text
E0 = union_f binom(S_f, 2)
D subset E0
A subset binom(V, 2) minus E0
G = (V, (E0 minus D) union A)
S subset V covers every pair in D union A; k = |S|
U = V minus S
T_f = S_f intersect U
F_u = {f : u in S_f}
```

All pairs are unordered, distinct-endpoint, normalized simple pairs. `D` means currently absent base pairs, not deletion messages or missing occurrences from just one factor. `A` means currently live non-base pairs. Restoration of a base pair removes it from `D`. Snapshot/projection/authorization semantics must remain fixed. Canonical labels use original IDs, not factor IDs, compact indices, or DSU representatives.

The theorem requires a valid cover, not a minimum cover. If input is already a flat arbitrary graph, obtaining a small exact factorization is additional work, not an oracle supplied by the theorem. No statement here extends to SCC, distances, cut capacities, or outputting an original-edge spanning forest without further witness work.

## Theorem And Proof

### 1. Outside Components

No changed edge has both endpoints in `U`; hence `G[U] = G0[U]`. Every nonempty `T_f` induces a connected clique, even when it is a singleton. Make a virtual node for each such factor. For each outside vertex `u`, union all nodes in `F_u`, using one as a streaming anchor. A vertex outside `S` in factor `f` guarantees `T_f` is nonempty.

After this pass, virtual-node roots correspond exactly to components of `G0[U]` that have any factor membership. A chain of intersecting outside factors supplies actual outside paths; conversely, each outside graph edge has a factor witness and maps within one such root. Factors that overlap only at a vertex of `S` must NOT be unioned in this pass. Vertices with `F_u` empty are separate base isolates and need no resident node.

### 2. Reattachment

For `s in S_f intersect S`, the base neighbors of `s` inside `T_f` are precisely all of `T_f`. Their surviving number is

```text
|T_f| - |N_D(s) intersect T_f|.
```

Thus unioning the explicit node for `s` with `f` iff this number is positive is exact. A positive count witnesses at least one real current edge into that outside component; it does not promise that every original pair survives. A zero count justifies no union for that particular factor. Another factor can still provide connectivity.

First scan `D_s`, marking forbidden core neighbors in generation-stamped flags and counting `d_out(s)=|N_D(s) intersect U|`. Write `m_f=|T_f|`. If `m_f>d_out(s)`, attachment is certain: even all outside deletions cannot remove every neighbor in this factor. The inequality must be strict. If `m_f=0`, attachment is impossible. Only factors with `0<m_f<=d_out(s)` are uncertain.

If `s` has any uncertain factor, replay `D_s`; for each outside neighbor `u`, replay `F_u` and increment only uncertain factors stamped for `s`. Each relevant `(s,u,f)` must be counted once. The exact local survivor count then resolves every uncertain attachment. If there are no uncertain factors, make no deletion-neighbor membership lookups. This is a valid one-sided pigeonhole certificate; **it does not license substituting global degree for the local count when the certificate fails**.

Call `(s,f)` **blocked** when `s in S_f intersect S` and `s` has no surviving base neighbor in `T_f`, including every `m_f=0` case. Enumerate the core posting `S_f intersect S` only for blocked `(s,f)`, using the already established forbidden flags to suppress deleted pairs. Reuse stamped candidate flags and a touched list to avoid scanning/resetting all `k` entries after each `s`.

**Blocked-enumeration lemma.** Consider a surviving core edge `st` and any factor `f` witnessing it. If both endpoints have surviving outside neighbors in `f`, they are connected through the nonempty connected `T_f`, regardless of whether their chosen outside neighbors coincide. Otherwise at least one endpoint is blocked and its row enumerates `st`. Thus every surviving core edge is represented by a path, even though some are never enumerated. Every performed union still lifts to a surviving edge or outside path, so there are no false merges either. An endpoint attaching to some *other* factor is not a reason to skip this factor's blocked posting. Requiring both endpoints to be blocked would be wrong.

The final DSU therefore has exactly the quotient's partition. It need not store that quotient's full edge set. Decisions may be processed in any core-row order: a connection supplied by the other endpoint's later row suffices for final correctness. Do not emit final labels until all rows and insertions are finished.

### 3. Insertions And Incidence-Free Isolates

Process core-core insertions directly. For each distinct outside insertion endpoint `u`, stream its inserted neighbors `I_u subset S`; a valid cover guarantees this containment.

* If `F_u` is nonempty, union every member of `I_u` with any one incident factor node. The outside pass already connected all incident factors.
* If `F_u` is empty and `I_u` is nonempty, union all members of `I_u` to one chosen member and reduce the original ID of `u` into that root's canonical minimum. No node for `u` is necessary: the path `s-u-t` induces exactly these connectivity unions. If there is only one neighbor, still reduce `u` into its minimum.
* If both sets are empty, `u` remains isolated and eventually emits its own ID.

Eliminating these isolate stars preserves connectivity, but not distance or edge count. There can be arbitrarily many such outside vertices with constant `k`; grouping and replaying insertion records is essential. One cannot retain a map for all of them inside the claimed query bound.

### 4. Complete Output And Minima

Initialize each core node's minimum from its original ID. On the first outside row pass, contribute `u` to the root of an incident factor, if one exists. Include incidence-free inserted vertices as above. Every union combines both roots' minima, including unions occurring after a minimum was first recorded.

After ALL unions, a second full vertex scan emits exactly one row per original vertex:

```text
v in S:                   minimum[find(core(v))]
v outside, F_v nonempty:   minimum[find(any f in F_v)]
v outside, F_v empty,
  I_v nonempty:            minimum[find(any s in I_v)]
otherwise:                v
```

Use a repeated merge with insertion groups, or a paid index lookup, for the third case. The root exists in every non-isolate case and has at least one original-ID contribution. Each component's contributions are precisely its vertices, possibly with harmless repetition of minima. Associativity of `min` proves canonical labels. A dictionary of all labels in the finite checker is only the test harness's materialized output; production must stream/backpressure or charge an output spool.

This is a proof for **all original IDs**, including components that do not meet `S`. A torso on `S` alone would lose those components and their output ownership.

## Source Processing And Costs

### Greedy Cover

Stream the complete normalized `D union A`. Maintain only the selected endpoint set `S`, initially empty. For a pair `uv`, if neither endpoint is already selected, select both. The implicitly selected pairs form a matching: no selected pair shares an endpoint with another. At the end every pair is covered, since a skipped pair already had a selected endpoint. Every vertex cover must hit each selected matching edge separately, so `|S| = 2|M| <= 2*tau(D union A)`.

This uses `O(k)` stored IDs, with expected constant-time dictionary membership or deterministic logarithmic membership. It still reads every normalized change record. No `n`-bit matched array is necessary. Dynamic hash capacity, resizing peak, and per-ID overhead must be admitted in physical bytes; stop and reject/fallback if the growing cover exceeds the reservation. A maximal matching is not a maximum matching and does not minimize `F`, `C_block`, or `J_uncertain`.

Normalization from source events can need external sorting, multiplicity checks, and snapshot comparison. A one-pass matching over an unnormalized event history may cover a superset, but its 2-approximation is NOT for the current change graph. If the source only announces a changed factor rather than enumerating changed canonical pairs, generating/normalizing those pair changes can itself be quadratic. This proposal assumes that cost is paid, not bypassed.

### External Views And Query Arrays

Required external access includes original-ID rows with factor memberships, factor-to-core postings `S_f intersect S`, replayable `F_s`, deletion rows `D_s`, and insertion groups by outside endpoint (plus core-core insertions). Orienting core-core deletions into two endpoint rows is a constant-factor copy, not a deletion-to-every-factor occurrence table. Core-to-outside deletions need only the core-oriented row once `S` is known. All building, sorting, offsets, index lookups, and coexisting generations count.

Keep a DSU/minimum structure of at most `F+k` slots, `F` outside sizes, `F` uncertain-factor stamps and deletion counters, a dictionary of `k` core IDs, two `k` generation-flag arrays for forbidden/candidate entries, and a touched list of at most `k` entries. For one core vertex at a time:

1. Stream `D_s`, marking forbidden core neighbors and counting outside deletions.
2. Stream `F_s`. Attach immediately when `m_f>d_out(s)`; stamp/zero only uncertain factors. Core-only factors require no membership-count join.
3. Only if an uncertain factor exists, replay `D_s` and join outside neighbors to `F_u`, counting stamped factors. Resolve their exact attachment status.
4. Replay `F_s`, scanning core postings only for blocked memberships. Append a core candidate only if not forbidden and not already candidate-stamped this generation. Union only the touched candidates; clear the touched list, not the entire flag arrays.

Neither all deletion neighbors nor a `k by k` matrix nor all core postings belong in RAM. Generation wraparound must trigger a controlled full stamp clear, not alias a previous generation. Deduplicate source membership rows and deletion pairs before counting, and use counters wide enough for admitted row lengths.

Let `L = sum_f |S_f|`, `r_f = |S_f intersect S|`, `b_f` be the number of blocked core members of factor `f`, and `Q` be the core vertices having at least one uncertain factor. Define

```text
C_block = sum_f r_f * b_f
J_uncertain = sum_{s in Q} sum_{u in N_D(s) intersect U} |F_u|.
```

With the stated views, a conservative logical item-work bound is

```text
O(n + L + F + k + |D| + |A| + C_block + J_uncertain)
```

before dictionary costs and DSU factors. Rank/path-compressed unions add the usual inverse-Ackermann amortized factor to union/find work. There is **no compulsory `k^2` scan/reset term**. Stamping/replaying `F_s` costs `sum r_f <= L` even when `C_block=0`; touched-list work is bounded by emitted posting items. The first and conditional second deletion scans together remain `O(|D|)` items with core-core orientation. The insertion joins can be merged with full vertex scans; even separately, reading `F_u` once per distinct outside insertion endpoint costs at most `L` membership items.

For positive-size outside factors, define the mathematical count

```text
P_f = sum_{s in S_f intersect S} |N_D(s) intersect T_f|.
b_f * m_f = sum_{blocked s in S_f} |N_D(s) intersect T_f| <= P_f.
sum_{f:m_f>0} b_f*m_f <= sum_f P_f <= J_all,
```

where `J_all` is the old unconditional membership-probe count. The equality is per-factor and counts **occurrences**, not distinct deletion pairs. `sum P_f` need not be `O(|D|)`. More tightly, blocked positive-size memberships all belong to uncertain rows, so their occurrences also inject into the executed `J_uncertain` visits. Nevertheless, `C_block` weights them by `r_f`, not `m_f`: only if `r_f/m_f` is bounded can this argument bound their core scans by a constant times the negative occurrences. Factors with `m_f=0` have `b_f=r_f`, contribute `r_f^2`, and cannot be charged to outside deletions at all. Overlapping factors can duplicate core postings despite candidate deduplication. Retain `C_block` explicitly.

Even with a greedy cover, the ratio matters: take one clique on `k` core vertices and one outside vertex `u`, with even `k>=4`. Delete a perfect matching among the cores, then `s-u` for one matched core `s`. Processing matching edges first gives exactly that core cover. Only `s` is blocked, so `C_block=k` but `J_uncertain=1`. With no outside vertex and just the deleted matching, `C_block=k^2` and `J_uncertain=0`. These are costs of this schedule, not lower bounds against complement search or other algorithms.

This is NOT an I/O-page or wall-time bound: small random indexed reads can cost a page each. Charge probes, repeated posting reads, cache policy, validation, source bytes, scratch and prepared-view bytes separately. On-the-fly `J_uncertain` processing avoids retaining an expanded deletion-occurrence table; it does not make that work disappear. The old example with factors `{s,u,x_i}`, `D={su}`, and `S={s}` now has `J_uncertain=C_block=0`, since every outside size is 2. To force amplified joins with distinct factors, use a factor `{s,u}` and factors `{u,x_i}`: the first is uncertain, so the single deletion joins all of `F_u` and `J_uncertain=F` even though most visited factors are not incident to `s`.

State is `O(F+k)` machine words plus explicitly budgeted service buffers, not `O(F+k)` bytes. `F` can be as large as the number of base edges, or inflated by duplicate factor identities, even when `k=0`. Counter, offset, and original-ID widths matter. The `F` reservation itself can force rejection. There is no measured 4 GB result here, and input/prepared/retained/output size is not bounded by the query-state expression.

## False Shortcuts

Use `sa` for unordered pair `{s,a}`. All unlisted pairs are absent unless implied by a listed factor.

| Shortcut | Concrete case | Correct result / failure |
| --- | --- | --- |
| Replace local missing count with global deletion degree | Factors `{s,a}`, `{s,b}`; `S={s}`; `D={sa}` | `{s,b}` stays connected; `a` isolates. Degree 1 wrongly suppresses the second attachment. |
| Count one deletion in only one factor | Two factor identities both `{s,a}`; `S={s}`; `D={sa}` | Both vertices isolate. An uncounted second factor falsely restores the pair. |
| Union factors through a core vertex before repair | Factors `{s,a}`, `{s,b}`; `S={s}`; `D={sa}` | Outside `a,b` are distinct components. Original factor connectivity through `s` cannot be reused. |
| Use `>=` instead of `>` for reattachment | Factor `{s,a}`; `S={s}`; `D={sa}` | Equality means zero surviving neighbors, not an edge. |
| Treat an empty outside factor as a live connector | Factor `{s,t}`; `S={s,t}`; `D={st}` | Two isolates. A virtual empty factor connecting both cores is unsound. |
| Ignore core-only factors altogether | Factor `{s,t}`; `S={s,t}`; `D=A=empty` | One component. Core candidates must include factors whose outside set is empty. |
| Ignore already marked forbidden flags when setting candidates | Two factors `{s,t}`; `S={s,t}`; `D={st}` | A later posting cannot reactivate the deleted candidate. Early deletion marking is safe only if all posting scans honor it. |
| Ignore the condition `s in S_f` | Factors `{s,a}`, `{b,c}`; `S={s}`; no changes | An unrelated outside factor is not a neighbor merely because its missing count is zero. |
| Cover only deletions | No factors; `D=empty`; `A={ab}`; `S=empty` | The cover premise fails. Both outside endpoints would incorrectly pass through as isolates. |
| Attach only one core neighbor of an outside isolate | No factors; `S={10,20}`; `A={1-10,1-20}` | All three vertices connect through 1. Its inserted neighborhood must be unioned. |
| Union isolate neighbors but omit its ID from minima | No factors; `S={10}`; `A={1-10}` | Both labels must be 1, not 10. |
| Use final DSU root number as label | Factor `{90,7}`; no changes | Both labels must be 7 even if a factor slot or vertex 90 becomes DSU root. |
| Count repeated deletion messages | Factor `{s,a,b}`; `S={s}`; source repeats deletion `sa` | Surviving `sb` attaches `s`; treating two messages as two distinct missing neighbors gives a false split. |
| Pigeonhole attach on `m_f >= d_out(s)` | Factor `{s,u}`; `D={su}`; `S={s}` | Equality can mean every outside neighbor was deleted. |
| Skip all joins because one factor passed pigeonhole | Factors `{s,u,v}`, `{s,w}`; `D={sw}`; `S={s}` | The second factor is still uncertain and blocked. |
| Enumerate only when both core endpoints are blocked | Factor `{s,t,u}`; `D={su}`; `S={s,t}` | `s` is blocked, `t` attaches, and surviving `st` is necessary to reconnect `s`. |
| Decide posting scans by whether `s` attaches anywhere | Factors `{s,u}`, `{t,v}`, `{s,t}`; no changes; `S={s,t}` | Both endpoints attach elsewhere, but the core-only factor is the sole bridge between their outside components. |
| Charge all core scans to distinct outside deletions | One factor on even `k>=4` vertices; delete a perfect matching, whose greedy endpoint cover is all vertices | `C_block=k^2` and `J_uncertain=0`. The source incidences also remain paid. |

An outside vertex in a singleton factor is not incidence-free. It follows the factor branch even if it has base degree zero. Conversely, an incidence-free outside vertex cannot have a base deletion incident to it under the normalized contract.

## Strong Forest Countercontrol

### Single Clique

In `K_n`, use a path spanning tree. Deletions incident only to one center cut at most two path edges; for a cover of size `k`, at most `2k` tree edges fail. The center-star deletion is not a separation from competent forest methods, regardless of how many non-tree edges disappear. The greedy endpoint cover of one nonempty deletion star has size two, not necessarily one; this does not change the conclusion.

### General Clique Union: A Stronger Bound

Fix any **true undirected DFS forest** `T` of `G0` before seeing `D`, `A`, or `S`. Ignore isolated one-vertex trees for the leaf argument. Let `l` be the total number of rooted leaves in the remaining trees, and `c` their number of trees.

1. Distinct rooted DFS leaves are incomparable in ancestor order. Undirected DFS has no cross edge between incomparable vertices, so two such leaves cannot share a clique factor.
2. Every nontrivial-tree leaf belongs to a factor containing its parent edge. Choosing one such factor per leaf is injective. Therefore `l <= F`.
3. Write `child(v)` for rooted child count. Summing `max(child(v)-1,0)` over nontrivial trees gives exactly `l-c`. Also `deg_T(v) <= 2 + max(child(v)-1,0)`. Isolates have degree zero.
4. Hence, simultaneously for every `S`,

```text
sum_{s in S} deg_T(s) <= 2k + l - c <= 2k + F
|D intersect E(T)| <= sum_{s in S} deg_T(s) <= 2k + F.
```

This is an independent derivation from the DFS ancestor property, not a theorem attributed to a paper below. It applies even with overlapping/duplicate factors, an arbitrary cover, and adversarial deletions after forest selection. It is stronger than assuming only a bounded maximum tree degree. The finite checker also checks these inequalities on every tested snapshot/cover, with additional randomized DFS orders.

There are at most `F` base components having any factor membership. All remaining base components are incidence-free singleton vertices. At most `k` of those singletons belong to `S`; eliminate outside incidence-free insertion stars exactly as in the kernel. Thus a suitably adapted forest solver needs at most

```text
q <= |D intersect E(T)| + F + k <= 2F + 3k
```

explicit surviving fragments, even including all factor-supported base components instead of only active ones. Unaffected incidence-free isolates stay implicit. This does not supply replacement edges for free: the forest method must still find surviving interfragment edges and pay for its preparation, descriptors, filtering and output. It **does** defeat a general claim that only the proposed kernel can achieve `O(F+k)` mutable union state.

Possible narrower experimental distinction: the factor kernel needs no persistent per-vertex parent/Euler/component ownership arrays beyond its admitted incidence/ID views; the cited forest route does. Compare those extra prepared bytes and actual query/build traffic. Do not claim a lower bound for every alternative forest implementation. A source-aware DFS construction, bounded-memory implementation, and repair schedule remain costs, not automatic free consequences of existence. A fresh `S`-aware spanning forest also gives a contraction-based small-state control, but rebuilding it at every query is not a fair immutable-preparation comparison.

## Inspected Primary Literature

Access recorded on 2026-09-21. Comparisons below separate what the source actually establishes from this review's inference. No conclusion relies on secondary generated summaries. No exhaustive novelty search or priority claim is made.

### Annotated Torso: Closest Structural Match

Rene van Bevern, Andreas Emil Feldmann, Manuel Sorge, Ondrej Suchy, *On the Parameterized Complexity of Computing Balanced Partitions in Graphs*, [author preprint v2, 2014](https://arxiv.org/pdf/1312.7014v2). **Inspected full-text passages:** Section 2.3.1, Definition 2 and Lemmas 1 and 3, PDF pages 13-16. Definition 2 contracts components outside a retained set and keeps the mapping from original vertices. Lemma 1 gives component correspondence; Lemma 3 constructs this annotated torso in linear explicit-graph time.

**Inference:** the construction computes the connectivity of the annotated torso of `G0-D` relative to `S` through incidence, then handles insertions and implicit/eliminated isolates. Blocked-only enumeration preserves that torso's partition, not all its edges. This is a close conceptual predecessor, not just keyword similarity. The paper's explicit adjacency and original-vertex mappings do not establish the external `O(F+k)` workspace schedule or `C_block+J_uncertain` accounting.

Daniel Marx, Barry O'Sullivan, Igor Razgon, *Finding Small Separators in Linear Time via Treewidth Reduction*, [2011 preprint](https://arxiv.org/pdf/1110.4765), published 2013. **Inspected:** Section 2.3, Definition 2.5 and the adjacent preservation statement. Ordinary torsos replace outside-component neighborhoods by cliques. **Inference:** a torso-only explanation misses untouched outside components and original-ID reconstruction; the annotated variant above is the closer match.

### Sensitivity And Repairable Hypergraphs

Ran Duan and Seth Pettie, *Connectivity Oracles for Graphs Subject to Vertex Failures*, [2017 revised full text](https://arxiv.org/pdf/1607.06865), later SIAM J. Comput. 2020. **Inspected:** introduction and Section 2, Theorem 2.1, especially PDF pages 3 and 6-8. Its Euler-tour structure separates failed tree edges from non-tree failures and reconnects resulting trees; low spanning-tree degree is explicitly important. It preprocesses a fixed graph for failure batches and later pair queries.

**Inference:** contracting known-connected pieces and working on failure-dependent fragments is established sensitivity-oracle practice. The present `S` covers failed AND inserted edges and remains in the output, so it is not literally a failed-vertex set. Canonical all-vertex output, native factor input, and external storage need separate analysis; pair-query timings are not full-output timings. This source reinforces the need for the strong forest control above, but does not itself assert that control's clique-cover bound.

Yaowei Long and Thatchaphol Saranurak, *Near-Optimal Deterministic Vertex-Failure Connectivity Oracles*, FOCS 2022, [proceedings full text](https://ieee-focs.org/FOCS-2022-Papers/pdfs/FOCS2022-4Bu7jGV9xIcveUWYj3oWoi/551900b002/551900b002.pdf), [author preprint record](https://arxiv.org/abs/2205.03930). **Inspected:** Section III.G, printed page 1008, and surrounding repair construction. It explicitly interprets its methods as repairable hypergraph-to-graph connectivity sparsifiers, preserving each surviving hyperedge's connectivity with a sparse graph plus repair edges after vertex failures.

**Inference:** avoiding dense hyperedge expansion while repairing connectivity under failures is not a new high-level idea. Their failed vertices and repair-edge sparsifiers differ from present pair deletions with a retained endpoint cover and factor-node state. Its bounds do not transfer by merely substituting `k` for a vertex-failure count.

### Hypergraph Incidence And Complement Search

M. Amin Bahmanian and Mateja Sajna, *Hypergraphs: Connection and Separation*, [2015 full text](https://arxiv.org/pdf/1504.04274). **Inspected:** Lemma 3.6 and Theorem 3.12, PDF pages 12-15. Walk/path correspondence and incidence-graph connectivity establish the standard outside-incidence foundation; the discussion explicitly distinguishes empty hyperedges. **Inference:** native clique/incidence WCC belongs to established hypergraph connectivity, already reflected by local D06. Omitting empty outside-factor connector nodes is essential, while original isolated vertices still belong in the answer.

Hiro Ito and Mitsuo Yokoyama, *Linear Time Algorithms for Graph Search and Connectivity Determination on Complement Graphs*, IPL 66(4), 1998, [publisher record and abstract](https://www.sciencedirect.com/science/article/pii/S0020019098000714), [DOI](https://doi.org/10.1016/S0020-0190(98)00071-4). **Access scope: publisher abstract retrieved through search; direct open failed; no full-text inspection.** The abstract states linear-time BFS/DFS and connectivity-related constructions on a graph's complement. For one clique on the full universe, `G0-D` is the complement of `(V,D)`. Thus avoiding quadratic expansion in the one-clique case is old; this abstract alone does not settle small `O(k)` workspace.

Elias Dahlhaus, Jens Gustedt, Ross M. McConnell, *Partially Complemented Representations of Digraphs*, DMTCS 5, 2002, [author-hosted full text](https://www.cs.colostate.edu/~rmm/cstacks.pdf). **Inspected:** Definition 1.1 and Theorem 1, printed pages 149-150. Row-complemented representations support BFS/DFS and component computations in time proportional to the stored graph rather than its expanded counterpart.

**Inference:** representation-aware search over absent edges is established. Arbitrary overlapping clique factors minus global pairs are not immediately a small row-complemented representation of the original vertices: which non-neighbors must be listed can be large. A useful special-case baseline, not a proved direct subsumption of `C_block+J_uncertain` and `O(F+k)` query state.

### Recent Representation-Aware Baseline

Ahammed Ullah and Alex Pothen, *Succinct Graph Representations and Algorithmic Applications*, [primary arXiv record, April 2026](https://arxiv.org/abs/2604.28096v1). **Access scope: primary abstract inspected; PDF/HTML body retrieval was unreliable and no algorithm section was successfully inspected.** The abstract defines a clique cover plus its incidence dual and states incidence-size-time algorithms for connected components, BFS/DFS forests, and maximal matching. This is directly relevant to the base representation and to a source-aware DFS baseline.

Do not infer its deletion handling, physical RAM, or all-ID output schedule from the abstract. Full-text comparison is an explicit remaining novelty-review item, not evidence that those features are absent. Its reported experimental speedups are not reproduced or borrowed here.

### Classification

The mathematical kernel is best described as an **incidence-native annotated-torso computation for a cover of normalized edge changes**, with standard contraction, complement counting, and connectivity-preserving elimination. `S` is a modulator to an unchanged induced subgraph, not necessarily a modulator to a disjoint union of cliques: overlapping outside factors can form arbitrary chains. With `F` unrestricted even `k=0` leaves an arbitrarily large kernel. Claim the explicit query schedule and conditional storage/traffic tradeoff as an engineering candidate; do not claim a new connectivity principle, a `k`-only kernelization, or established publication-level novelty.

Within the inspected passages I did not find the precise combination of a change-edge endpoint cover, row-degree attachment certificate, blocked-factor core scans, `O(F+k)` query workspace, and streamed all-ID minima. That is a bounded search result, not proof of absence. Generic pigeonhole reasoning and omitting edges whose endpoints already have certified paths are elementary; the stronger composite schedule must be compared as a whole before assigning novelty.

## Independent Finite Checker

The single Python block below is standalone, imports only the standard library, and never imports, executes, or reads lead code. The reference oracle explicitly expands base clique pairs and traverses the resulting current graph. The candidate independently uses factor DSU nodes, a first deletion pass, conditional second-pass membership joins, blocked-only postings, generation-stamped forbidden/candidate flags and touched lists, streamed-style insertion groups, minimum propagation, and a separate output pass. In printed results and internal stats, `C` means `C_block` and `J` means `J_uncertain` (the lead's `J_U`), not the initial draft's costs.

`views` simulates already prepared on-disk views in RAM for tiny fixtures; its storage, the expanded oracle, and materialized test output are NOT included in the candidate's logical query-state claim. No physical-memory benchmark is implied. The kernel's mutable arrays are `O(F+k)`, and it does not create a deletion set or a quadratic core matrix. Normalization checks are performed by the fixture preparer, outside the query kernel. Fixed-width overflow, disk faults, stale indexes, epoch validation, and OS-level memory are not tested.

Coverage: all universes of size 0 through 4 using non-monotone original IDs; every multiset of 0, 1, or 2 factors including empty, singleton, and duplicate factors; every current simple graph; every valid change cover. Extra seeded random cases use up to 9 vertices and 8 factors, greedy cover order variation, and randomized DFS root order/ID labels. Dedicated mutants must fail on their counterexamples, so equality of two implementations is not the only control. The checker verifies greedy maximal-matching coverage/2-approximation by brute force on the exhaustive fixtures, exact whole-ID label dictionaries, measured logical `C`/`J`, the blocked-occurrence bound, and the DFS bound.

Run without creating any other file, from the repository root:

```sh
awk '/^```python$/{p=1;next} p && /^```$/{exit} p' research_algorithms_20260920/Connectivity-Fault-Cover-Review.md | python3 -B -
```

```python
from itertools import combinations, combinations_with_replacement
from random import Random


def normalize_edge_pair_exact(left, right):
    assert left != right
    return (min(left, right), max(left, right))


def enumerate_mask_subsets_exact(items):
    items = tuple(items)
    for mask in range(1 << len(items)):
        yield tuple(item for index, item in enumerate(items) if mask >> index & 1)


def expand_factor_pairs_exact(factors):
    return {normalize_edge_pair_exact(u, v)
            for factor in factors for u, v in combinations(factor, 2)}


def compute_expanded_labels_exact(vertices, factors, deleted, added):
    adjacency = {vertex: set() for vertex in vertices}
    for left, right in (expand_factor_pairs_exact(factors) - set(deleted)) | set(added):
        adjacency[left].add(right)
        adjacency[right].add(left)
    labels = {}
    for vertex in vertices:
        if vertex in labels:
            continue
        reached, pending = {vertex}, [vertex]
        while pending:
            for neighbor in adjacency[pending.pop()]:
                if neighbor not in reached:
                    reached.add(neighbor)
                    pending.append(neighbor)
        labels.update((item, min(reached)) for item in reached)
    return labels


def select_greedy_cover_exact(edges):
    selected = set()
    for left, right in edges:
        if left not in selected and right not in selected:
            selected.update((left, right))
    return selected


def prepare_fixture_views_exact(vertices, factors, deleted, added, cover):
    universe, selected = set(vertices), set(cover)
    assert len(universe) == len(vertices) and selected <= universe
    assert all(len(set(factor)) == len(factor) and set(factor) <= universe
               for factor in factors)
    base = expand_factor_pairs_exact(factors)
    assert len(set(deleted)) == len(deleted) and len(set(added)) == len(added)
    assert all(u < v and u in universe and v in universe for u, v in (*deleted, *added))
    assert set(deleted) <= base and not (set(added) & base)
    assert all(u in selected or v in selected for u, v in (*deleted, *added))
    rows = {u: tuple(f for f, members in enumerate(factors) if u in members)
            for u in vertices}
    core = tuple(tuple(u for u in members if u in selected) for members in factors)
    failures = {s: tuple(v if s == u else u for u, v in deleted if s in (u, v))
                for s in selected}
    inserted_core, inserted_outside = [], {}
    for u, v in added:
        if u in selected and v in selected:
            inserted_core.append((u, v))
        else:
            outside, inside = (v, u) if u in selected else (u, v)
            inserted_outside.setdefault(outside, []).append(inside)
    return (tuple(vertices), rows, core, failures, tuple(inserted_core),
            {u: tuple(neighbors) for u, neighbors in inserted_outside.items()})


def compute_factor_kernel_exact(views, cover, stats, mutant=None):
    vertices, rows, core, failures, inserted_core, inserted_outside = views
    selected = tuple(cover)
    factor_count, core_count = len(core), len(selected)
    position = {s: i for i, s in enumerate(selected)}
    parent = list(range(factor_count + core_count))
    rank = [0] * len(parent)
    minimum = [None] * factor_count + list(selected)
    outside_size = [0] * factor_count
    stamps, missing = [0] * factor_count, [0] * factor_count
    forbidden, candidates = [0] * core_count, [0] * core_count
    stats.update(C=0, J=0, slots=len(parent))

    def find_parent_root_exact(node):
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def reduce_root_minimum_exact(node, value):
        root = find_parent_root_exact(node)
        if minimum[root] is None or value < minimum[root]:
            minimum[root] = value

    def union_parent_roots_exact(left, right):
        left, right = find_parent_root_exact(left), find_parent_root_exact(right)
        if left == right:
            return
        if rank[left] < rank[right]:
            left, right = right, left
        parent[right] = left
        if rank[left] == rank[right]:
            rank[left] += 1
        if minimum[right] is not None:
            reduce_root_minimum_exact(left, minimum[right])

    for vertex in vertices:
        memberships = rows[vertex]
        if vertex in position:
            if mutant == "through_core" and memberships:
                for factor in memberships:
                    union_parent_roots_exact(memberships[0], factor)
            continue
        for factor in memberships:
            outside_size[factor] += 1
            union_parent_roots_exact(memberships[0], factor)
        if memberships:
            reduce_root_minimum_exact(memberships[0], vertex)

    for generation, vertex in enumerate(selected, 1):
        outside_deletions = 0
        for neighbor in failures[vertex]:
            if neighbor in position:
                forbidden[position[neighbor]] = generation
            else:
                outside_deletions += 1
        node = factor_count + position[vertex]
        uncertain, certain = False, False
        for factor in rows[vertex]:
            immediate = outside_size[factor] > outside_deletions
            if mutant == "nonstrict":
                immediate = outside_size[factor] >= outside_deletions
            if immediate:
                union_parent_roots_exact(node, factor)
                certain = True
            elif outside_size[factor] > 0:
                stamps[factor], missing[factor] = generation, 0
                uncertain = True
        if uncertain and not (mutant == "skip_uncertain" and certain):
            for neighbor in failures[vertex]:
                if neighbor in position:
                    continue
                counted = False
                for factor in rows[neighbor]:
                    stats["J"] += 1
                    if stamps[factor] == generation:
                        if mutant != "one_factor" or not counted:
                            missing[factor] += 1
                        counted = True
        touched = []
        row_attaches = mutant == "any_factor_attach" and (certain or any(
            stamps[f] == generation and missing[f] < outside_size[f]
            for f in rows[vertex]))
        for factor in rows[vertex]:
            immediate = outside_size[factor] > outside_deletions
            if mutant == "nonstrict":
                immediate = outside_size[factor] >= outside_deletions
            if immediate:
                continue
            lost = outside_deletions if mutant == "global_degree" else missing[factor]
            if outside_size[factor] > 0 and outside_size[factor] > lost:
                union_parent_roots_exact(node, factor)
                continue
            if mutant == "omit_core" or (mutant == "any_factor_attach" and row_attaches):
                continue
            for neighbor in core[factor]:
                stats["C"] += 1
                if neighbor == vertex:
                    continue
                index = position[neighbor]
                if mutant == "both_blocked" and any(
                        u not in position and factor in rows[u] and u not in failures[neighbor]
                        for u in vertices):
                    continue
                if forbidden[index] != generation and candidates[index] != generation:
                    candidates[index] = generation
                    touched.append(index)
        for index in touched:
            union_parent_roots_exact(node, factor_count + index)

    for left, right in inserted_core:
        union_parent_roots_exact(factor_count + position[left], factor_count + position[right])
    for outside, neighbors in inserted_outside.items():
        memberships = rows[outside]
        anchor = memberships[0] if memberships else factor_count + position[neighbors[0]]
        for neighbor in neighbors:
            if mutant == "omit_isolate_bridge" and not memberships:
                break
            union_parent_roots_exact(anchor, factor_count + position[neighbor])
        if memberships or mutant != "omit_isolate_min":
            reduce_root_minimum_exact(anchor, outside)

    for vertex in vertices:
        if vertex in position:
            node = factor_count + position[vertex]
        elif rows[vertex]:
            node = rows[vertex][0]
        elif vertex in inserted_outside:
            node = factor_count + position[inserted_outside[vertex][0]]
        else:
            yield vertex, vertex
            continue
        assert minimum[find_parent_root_exact(node)] is not None
        yield vertex, minimum[find_parent_root_exact(node)]


def build_depth_forest_exact(vertices, base):
    adjacency = {u: [] for u in vertices}
    for u, v in sorted(base):
        adjacency[u].append(v)
        adjacency[v].append(u)
    visited, tree, child_count = set(), set(), {u: 0 for u in vertices}

    def visit_depth_vertex_exact(vertex):
        visited.add(vertex)
        for neighbor in adjacency[vertex]:
            if neighbor not in visited:
                child_count[vertex] += 1
                tree.add(normalize_edge_pair_exact(vertex, neighbor))
                visit_depth_vertex_exact(neighbor)

    for vertex in vertices:
        if vertex not in visited:
            visit_depth_vertex_exact(vertex)
    leaves = sum(bool(adjacency[u]) and child_count[u] == 0 for u in vertices)
    degree = {u: sum(u in edge for edge in tree) for u in vertices}
    return tree, leaves, degree


def check_single_snapshot_exact(vertices, factors, deleted, added, cover, forest=None):
    views = prepare_fixture_views_exact(vertices, factors, deleted, added, cover)
    stats = {}
    actual = dict(compute_factor_kernel_exact(views, cover, stats))
    expected = compute_expanded_labels_exact(vertices, factors, deleted, added)
    assert actual == expected, (vertices, factors, deleted, added, cover, actual, expected)
    selected = set(cover)
    outside = set(vertices) - selected
    negative = {s: {v if s == u else u for u, v in deleted if s in (u, v)}
                for s in selected}
    uncertain = {s for s in selected if any(
        s in factor and 0 < len(set(factor) & outside) <= len(negative[s] & outside)
        for factor in factors)}
    expected_join = expected_core = blocked_occurrences = 0
    for factor in factors:
        inside, exterior = set(factor) & selected, set(factor) & outside
        blocked = sum(exterior <= negative[s] for s in inside)
        expected_core += len(inside) * blocked
        occurrences = sum(len(exterior & negative[s]) for s in inside)
        assert blocked * len(exterior) <= occurrences
        blocked_occurrences += blocked * len(exterior)
    assert stats["C"] == expected_core
    for u, v in deleted:
        if (u in selected) != (v in selected):
            inside, exterior = (u, v) if u in selected else (v, u)
            if inside in uncertain:
                expected_join += sum(exterior in factor for factor in factors)
    assert stats["J"] == expected_join
    assert blocked_occurrences <= expected_join
    assert stats["slots"] == len(factors) + len(cover)
    tree, leaves, degree = forest or build_depth_forest_exact(vertices, expand_factor_pairs_exact(factors))
    assert leaves <= len(factors)
    assert sum(degree[s] for s in cover) <= 2 * len(cover) + len(factors)
    assert len(tree & set(deleted)) <= 2 * len(cover) + len(factors)
    return stats


def run_exhaustive_cases_exact():
    snapshots = checks = greedy_checks = 0
    for count in range(5):
        vertices = (61, 7, 103, 29)[:count]
        pairs = tuple(normalize_edge_pair_exact(u, v) for u, v in combinations(vertices, 2))
        subsets = tuple(enumerate_mask_subsets_exact(vertices))
        for factor_count in range(3):
            for factors in combinations_with_replacement(subsets, factor_count):
                base = expand_factor_pairs_exact(factors)
                forest = build_depth_forest_exact(vertices, base)
                for current in enumerate_mask_subsets_exact(pairs):
                    deleted, added = base - set(current), set(current) - base
                    changes = deleted | added
                    covers = tuple(cover for cover in subsets
                                   if all(u in cover or v in cover for u, v in changes))
                    optimum = min(map(len, covers))
                    for ordered in (sorted(changes), sorted(changes, reverse=True)):
                        greedy = select_greedy_cover_exact(ordered)
                        assert all(u in greedy or v in greedy for u, v in changes)
                        assert len(greedy) <= 2 * optimum
                        greedy_checks += 1
                    for cover in covers:
                        check_single_snapshot_exact(vertices, factors, deleted, added, cover, forest)
                        checks += 1
                    snapshots += 1
    print("exhaustive snapshots:", snapshots)
    print("exhaustive kernel/label/C/J/DFS checks:", checks)
    print("greedy coverage and 2-approx checks:", greedy_checks)


def run_randomized_cases_exact():
    random = Random(20260921)
    for unused in range(3000):
        vertices = tuple(random.sample(range(1, 1000), random.randrange(10)))
        factors = tuple(tuple(u for u in vertices if random.randrange(2))
                        for unused_factor in range(random.randrange(9)))
        base = expand_factor_pairs_exact(factors)
        current = {normalize_edge_pair_exact(u, v) for u, v in combinations(vertices, 2)
                   if random.randrange(2)}
        deleted, added = base - current, current - base
        changes = sorted(deleted | added)
        random.shuffle(changes)
        selected = select_greedy_cover_exact(changes)
        selected.update(u for u in vertices if random.randrange(5) == 0)
        check_single_snapshot_exact(vertices, factors, deleted, added, tuple(sorted(selected)))
    print("seeded random kernel/label/C/J/DFS checks:", 3000)


def run_adversarial_controls_exact():
    cases = (
        ("global_degree", (10, 20, 30), ((10, 20), (10, 30)), ((10, 20),), (), (10,)),
        ("one_factor", (10, 20), ((10, 20), (10, 20)), ((10, 20),), (), (10,)),
        ("through_core", (10, 20, 30), ((10, 20), (10, 30)), ((10, 20),), (), (10,)),
        ("nonstrict", (10, 20), ((10, 20),), ((10, 20),), (), (10,)),
        ("nonstrict", (10, 20), ((10, 20),), ((10, 20),), (), (10, 20)),
        ("omit_core", (10, 20), ((10, 20),), (), (), (10, 20)),
        ("omit_isolate_bridge", (1, 10, 20), (), (), ((1, 10), (1, 20)), (10, 20)),
        ("omit_isolate_min", (1, 10), (), (), ((1, 10),), (10,)),
        ("skip_uncertain", (10, 20, 30, 40), ((10, 20, 30), (10, 40)),
         ((10, 40),), (), (10,)),
        ("both_blocked", (10, 20, 30), ((10, 20, 30),), ((10, 30),), (), (10, 20)),
        ("any_factor_attach", (10, 20, 30, 40), ((10, 30), (20, 40), (10, 20)),
         (), (), (10, 20)),
    )
    for mutant, vertices, factors, deleted, added, cover in cases:
        check_single_snapshot_exact(vertices, factors, deleted, added, cover)
        views = prepare_fixture_views_exact(vertices, factors, deleted, added, cover)
        faulty = dict(compute_factor_kernel_exact(views, cover, {}, mutant))
        assert faulty != compute_expanded_labels_exact(vertices, factors, deleted, added), mutant
    rejections = (
        ((1, 2), (), (), ((1, 2),), ()),
        ((1, 2, 3), ((1, 2, 3),), ((1, 2), (1, 2)), (), (1,)),
        ((1, 2), ((1, 1, 2),), (), (), ()),
    )
    for arguments in rejections:
        try:
            prepare_fixture_views_exact(*arguments)
        except AssertionError:
            continue
        raise AssertionError("invalid input was accepted")
    factors = ((100, 200),) + tuple((200, index) for index in range(31))
    stats = check_single_snapshot_exact(tuple(range(31)) + (100, 200), factors,
                                        ((100, 200),), (), (100,))
    assert stats["J"] == 32 and stats["C"] == 1
    stats = check_single_snapshot_exact((10, 20, 1, 2), ((10, 20, 1, 2),),
                                        ((1, 10), (2, 20), (10, 20)), (), (10, 20))
    assert stats["J"] == 0 and stats["C"] == 0
    stats = check_single_snapshot_exact((10, 20, 30, 1, 2),
                                        ((10, 20, 30, 1, 2), (1, 2)),
                                        ((1, 10), (2, 10)), (), (10, 20, 30))
    assert stats["J"] == 4 and stats["C"] == 3
    matching = ((1, 2), (3, 4), (5, 6))
    deleted = matching + ((0, 1),)
    cover = tuple(sorted(select_greedy_cover_exact(deleted)))
    assert cover == (1, 2, 3, 4, 5, 6)
    stats = check_single_snapshot_exact(tuple(range(7)), (tuple(range(7)),),
                                        deleted, (), cover)
    assert stats["C"] == 6 and stats["J"] == 1
    stats = check_single_snapshot_exact(cover, (cover,), matching, (), cover)
    assert stats["C"] == 36 and stats["J"] == 0
    vertices = tuple(range(1, 102))
    added = tuple((1, u) for u in vertices if u != 1)
    stats = check_single_snapshot_exact(vertices, (), (), added, (1,))
    assert stats["slots"] == 1
    print("false-shortcut mutant cases rejected:", len(cases))
    print("invalid source/cover cases rejected:", len(rejections))
    print("join amplification: |D|=1, J_uncertain=32, C_block=1")
    print("pigeonhole floor: C_block=0, J_uncertain=0")
    print("blocked-core control: C_block=3, J_uncertain=4")
    print("greedy-cover cost controls: (C_block,J_uncertain)=(6,1),(36,0)")
    print("outside-isolate insertion fanout: |A|=100, DSU slots=1")


run_exhaustive_cases_exact()
run_randomized_cases_exact()
run_adversarial_controls_exact()
print("PASS: independent fault-cover review checker")
```

### Run Record

Executed with Python 3.9.6 using the extraction command above, on 2026-09-21. Exit status 0. The exact final block produced:

```text
exhaustive snapshots: 10191
exhaustive kernel/label/C/J/DFS checks: 85530
greedy coverage and 2-approx checks: 20382
seeded random kernel/label/C/J/DFS checks: 3000
false-shortcut mutant cases rejected: 11
invalid source/cover cases rejected: 3
join amplification: |D|=1, J_uncertain=32, C_block=1
pigeonhole floor: C_block=0, J_uncertain=0
blocked-core control: C_block=3, J_uncertain=4
greedy-cover cost controls: (C_block,J_uncertain)=(6,1),(36,0)
outside-isolate insertion fanout: |A|=100, DSU slots=1
PASS: independent fault-cover review checker
```

The exhaustive counts are this checker's own enumeration, not the lead's reported test counts. A passing run supports the bounded cases; the proofs above are the unbounded correctness argument. The script creates no files and performs no network access.

## Final Synthesis And Open Questions

Accept the exact partition/output theorem with the normalization, local membership counting, and replayable-view conditions above. Reject global-degree attachment shortcuts, uncharged factor/deletion expansion, omission of incidence-free inserted minima, and universal forest-state superiority. The annotated-torso predecessor is close enough that a novelty claim must focus on a genuinely distinct schedule or bound and survive a matched implementation comparison.

Remaining work for the lead: a physical byte reservation and complete source/view lifecycle; an actual factor-aware DFS/fragment competitor; measurements separating `C_block`, `J_uncertain`, index pages, preparation, and unavoidable output; and full-text inspection of the recent DCC paper before any strong novelty assertion. The prototype's trusted prepared-view boundary is accepted as a boundary, not mistaken for an implemented physical builder. The finite checker is a mathematical falsifier, not a production external-memory implementation or proof by exhaustion for unbounded inputs.

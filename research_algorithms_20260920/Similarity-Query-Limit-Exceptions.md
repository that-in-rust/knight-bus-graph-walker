# Query-Limit Exception Summaries For Exact Similarity Envelopes

Date: 2026-09-21. A05 research increment. Implemented count-summary prototype;
not a production graph engine, physical RAM measurement, universal speedup, or
established publication-priority claim. The completed independent mathematical
[Floor/Core Review](Similarity-Floor-Core-Review.md) supports the stated
representation, clipping and information bound; it did not inspect the code.

## Abstract

We consider the exact maximum Jaccard similarity of either of two hidden sets,
over all pairs consistent with an exact union, prescribed row sizes, and attained
maximum counts on a laminar feature tree. The earlier strict-orientation theorem
contracts additive regions to an O(r+1)-record core, where r counts strictly
subadditive internal capacities. Reusing that core previously retained a G-entry
leaf lookup table for future queries. We replace that table with at most 3r+1
ownership intervals and e query-sensitive exceptions, for queries whose count in
each leaf is at most a declared tau. The exact envelope is unchanged on this
domain. Retained state is O(r+e+1) integer records after paid O(G) preparation;
query arithmetic is O(r+s log(e+2)+(r+1)2^r) for s sorted nonzero leaf counts.

Two qualifications are substantive. First, omitting every fixed-floor exception admits a
regime in which the envelope collapses to the ordinary union/row-size bound: the
apparently strongest compression can discard all useful extra pruning. Second,
even r=0 and tau=1 can encode an arbitrary e-subset of the G leaves in the exact
query answers. This requires log2 binom(G,e) bits in a fixed-size self-contained
summary. The information argument is standard; the construction realizes it in
this particular attained-capacity model. The result is a query-domain/storage
tradeoff, not constant-memory exact similarity for all graphs.

Section 11 adds an implemented stronger representation: use the most frequent
saturated query signature in each component, retaining d deviations rather than
e fixed-floor exceptions. Then d<=e and retained state is O(r+d+1), at a paid
O(G log G) deterministic compilation cost. Zero modal deviations can retain
stronger bounds; it is not the floor-only regime. Most-frequent default coding
is explicitly credited to existing work.

## 1. Problem, Motivation And Scope

The graph application is filtering a block of exactly two target neighborhoods
before reading their bodies during set-similarity search. A block envelope is an
upper bound for the actual stored rows. An exact maximum over compatible pairs
is **not** the similarity of the original unknown pair, nor an entire top-k
answer. Candidate selection, exact surviving-body work, tie handling, original
IDs, and complete output remain necessary in a graph-query implementation.

Keep the contracts of [Orientation Recovery](Similarity-Orientation-Parameter-Recovery.md),
[Additive Core](Similarity-Additive-Region-Core.md), its
[independent review](Similarity-Additive-Core-Review.md), and
[Static Query Updates](Similarity-Static-Core-Query-Updates.md):

- U is the exact union, partitioned into G ordered leaf groups. H_j=|U_j|.
- C_v is the attained maximum of the two row counts in subtree v. At leaves,
  C_j <= H_j <= 2C_j. Write l_j=H_j-C_j, so 0<=l_j<=C_j.
- The major root size is u=C_root; the minor root size ell<=u is supplied.
  Both rows must be simultaneously realizable. Locally valid capacities alone
  do not guarantee this; the existing orientation solver checks it.
- Internal deficit D_v=C_left+C_right-C_v is nonnegative; r counts D_v>0.
- Query size a includes outside-union features. q_j counts query features in
  U_j, Q=sum q_j<=a. The new admission condition is q_j<=tau for every j.
- Scores use exact rational arithmetic. Zero overlap scores zero, including
  empty/empty. Infeasible metadata raises an error instead of returning a bound.

The prototype accepts balanced power-of-two trees with 1<=G<=256. The structural
proof uses an ordered binary tree and is not inherently capped at 256; extending
the executable parser and its resource admission is separate work.

## 2. Rejected Floor-Only Design

The tempting design was to retain a component floor f_B=min_{j in B} l_j and
admit q_j<=f_owner. Then both leaf reward terms equal q_j and no leaf exceptions
are needed. This is exact, but its pruning benefit disappears.

**Floor-collapse theorem.** For feasible metadata and q_j<=l_j at all queried
leaves, the exact envelope is Q/(a+ell-Q), with zero for Q=0.

Every compatible row contains at least l_j elements in leaf j. Hence its count
can accommodate all q_j query elements. Start with any realizing count pair,
choose the minor row's elements query-first within each group, and complete the
other row's elements to cover U. Counts and all attained maxima are preserved.
The minor row therefore achieves overlap Q. No row can exceed Q; the smaller
row size ell maximizes Jaccard at that overlap. The ordinary union-and-size
bound already gives this value. This proof is about interchangeable identities
within a supplied count summary, not the actual original row's overlap.

Also, a component *sum* L=sum l_j cannot replace each leaf's floor. Take two
leaves with C=(2,2), H=(2,4), additive root u=4, ell=2, a=1. Querying leaf 0 has
exact envelope 1/4; querying leaf 1 has envelope 1/2. Both have Q=1<=L=2.
Treating either leaf as regular from the sum alone is unsound. The retained
tests preserve both this counterexample and 552 floor-collapse instances.

## 3. The Exception Representation

Compile the existing additive core with an all-zero query. There are c<=2r+1
additive components, K=r+c<=3r+1 core nodes, and K-1 core edges. Component B starts
with the exact capped-linear record (L_B,U_B,L_B,0).

Choose a nonnegative integer tau before discarding leaf metadata. Retain:

1. The immutable core, capacities, component totals and root metadata.
2. Maximal half-open leaf intervals (start,end,owner), covering all G leaves.
3. For every leaf with l_j<tau, an exception (j,l_j,min(C_j,tau)).
4. A sorted exception-key index. The prototype duplicates those e keys in a
   tuple for binary search; this is counted, not hidden.
5. G and tau, but no original G-entry population/capacity/owner arrays.

For tau=0 only the empty in-union query is admitted and e=0. For increasing tau,
e(tau)=|{j:l_j<tau}| is monotone and can reach G. Wider query support can erase
the storage advantage. Multiple prepared profiles need shared-object accounting
or incur multiple retained copies; the prototype implements one profile.

### Ownership-Interval Lemma

There are at most 3r+1 ownership runs, even when one additive component's ordinary
leaves occur in several disjoint runs. For each strict node mark its subtree's
left boundary, child divider, and right boundary in the inorder leaf sequence.
Between consecutive marked positions, no strict ancestor can change its
inside/outside/left/right relationship to a leaf. Thus the nearest strict split
and resulting additive-component owner are constant. At most 3r cuts produce
at most 3r+1 runs; repeated/end boundaries only reduce the count.

One interior strict pair in G=8 attains four runs: outside-left, left child,
right child, outside-right. Two disjoint interior strict pairs in G=16 attain
seven. Nested or adjacent strict nodes and components with no ordinary leaves
do not invalidate the bound; zero-leaf components simply own no run.
The independent review strengthens this to an exact formula: one plus the number
of distinct internal cuts among all strict subtree starts/dividers/ends. Its
graph-connectivity checker covers arbitrary ordered full binary shapes, beyond
the prototype's balanced input format.

## 4. Exact Query Procedure And Preservation

The query stream supplies strictly increasing, unique (leaf_id,q) pairs with
positive builtin-integer q. Omitted leaves mean zero. Reserve the existing
orientation budget before touching the stream. Copy only the K core records.
Use a monotone owner-run cursor and binary search the exception keys per pair.

```text
ZERO-QUERY CORE + OWNER RUNS + EXCEPTIONS
                    |
                    v
         Read next sorted (leaf,q)
                    |
         q > tau? --+--> Refuse this profile
                    |
                    v
       Regular leaf: reward_low = reward_high = q
       Exception:    reward_low = min(q,l)
                     reward_high = min(q,clipped_C)
                    |
                    v
       owner.A -= reward_low; owner.B += reward_high
                    |
                    v
       Existing exact two-row orientation evaluator
```

For a regular leaf, l_j>=tau>=q_j and C_j>=l_j. Its two original terms
min(q_j,l_j) and min(q_j,C_j) both equal q_j. The leaf also automatically has
H_j>=q_j. For an exception, the retained l_j is exact. Clipping C at tau changes
neither min(q,C) nor validation q<=l+C for q<=tau: if C>=tau, every admitted q
fits; otherwise C was not changed. Therefore the resulting bundle fields are
exactly

```text
A_B = L_B - sum_{j in B} min(q_j,l_j)
B_B =       sum_{j in B} min(q_j,C_j).
```

They equal the dense query-dependent compiler field-for-field. Nothing else in
that compiler depends on q. The existing boundary lifting and both-row feasibility
arguments therefore apply, with no new approximation or lost companion check.
The score is maximized separately over either feasible row; the two marginal
optima need not occur in the same realizing pair.

Malformed streams, excess q, excess sum(q), infeasible metadata, interrupted
iterators and exhausted branch budgets never mutate the static object. There is
no silent fallback to a weaker number. An outer engine may explicitly choose an
ordinary bound or body read, but that integration is not implemented here.

## 5. Costs, Bits And Admission

| Stage | Paid work | Resident/extra count records |
| --- | --- | --- |
| Validate and compile | O(G) arithmetic, including ownership traversal | O(G) temporary input/compiler allowance |
| Retain one static profile | No query work | O(r+e+1), including duplicate exception keys |
| Prepare an s-pair query | O(r+s log(e+2)) | O(r+1) extra; no s-entry container |
| Evaluate all admitted orientations | O((r+1)2^r) | O(r+1) extra |

Thus this improves **retained summary space** over the previous static G-table,
not whole-builder peak memory. The compiler still creates G zero counts and
walks the original tree twice. Successful compilation releases input arrays
without relying on cyclic GC; retained exception tracebacks and caller buffers
are outside that successful-return ownership statement.

The query uses a bounded monotonically advancing owner cursor, not a scan of all
G groups. The branch reservation remains N=2^r and W=5K-2+2c<=19r+5 work units
per branch. The evaluator refuses N>branch_cap or N*W>work_cap before copying the
core or consuming the stream. W counts core-node/child/bundle visits; it does
not count query preparation, rational bit arithmetic, disk reads or wall time.

The record bound is not a bit-size kernel. If all retained original capacities
and union totals are at most N_count, a simple packed realization would need

```text
O((r+1)*(log(G+2)+log(N_count+2))
  + e*(log(G+2)+log(tau+2))) bits,
```

plus format headers and chosen indexing overhead. Query arithmetic also depends
on a's width, intermediate products and Fraction normalization. Python tuples,
dicts, proxies and arbitrary-precision integers are not this packed realization.
Clipping makes exception payload widths query-limited; the original aggregate
core still contains full counts. A symbolic 10^100 capacity is not a graph with
10^100 ingested features.

Known succinct indexable dictionaries could replace the sorted exception-key
tuple, using approximately log2 binom(G,e) bits plus lower-order indexing terms
and packed exception values, with the original core charged separately. This is
an unimplemented use of established machinery, not our dictionary innovation.
Construction, word-size assumptions and shared tables must be priced before
claiming its RAM-model query bounds in a real executor.

## 6. Nontrivial Family With Constant Exceptions

Let G>=8 be a power of two and M=10^12. Ordinary leaves have C=M and H=3M/2.
Leaves 2 and 3 instead have C=3,H=4; their common parent has capacity 4, a strict
deficit of 2. All other internal capacities are additive. Set both row sizes to
u=(G-2)M+4. These metadata are feasible: both rows occupy M items in each ordinary
group with M/2 shared, and their exceptional counts are (3,1) and (1,3).

Choose tau=2, q_2=q_3=2 and a=4. Then r=1,e=2,K=4 and there are four ownership
runs, independently of G. On each exceptional pair a row hits at most 3 query
items, attaining that value. The exact envelope is 3/(u+1), strictly below the
ordinary union/size bound 4/u. This is the same strict information recovery as
the full attained-capacity solver, now preserved by a small reusable summary.

The executed family uses G=8,16,32,64,128,256. Each query copies four core records,
does two owner lookups and two exception hits, and revisits no original tree.
There remain only two exception leaf records rather than G table records. These
are logical counts, not a 99%-RAM headline or a workload-distribution finding.
The retained record sizes grow logarithmically with the encoded counts and G.

## 7. Exception Positions Carry Necessary Information

Fix 0<e<G, tau=1, and exactly two features per leaf, H_j=2. Choose any set E of
e leaf positions. Give each j in E capacity C_j=2,l_j=0; all other leaves have
C_j=1,l_j=1. Make every internal capacity additive. Then

```text
r=0, u=G+e, ell=G-e, H_root=2G,
the sole core bundle is (G-e,G+e,G-e,0).
```

These aggregates, the exact feature union, feature-to-group mapping, query
domain and core are identical for all binom(G,e) choices. A realizing minor row
takes one feature in each nonexception leaf and none in E; the major row takes
the remaining union. Because ell equals the sum of mandatory lower counts,
every realizing minor row has those same occupancies.

A singleton query at leaf j has exact envelope 1/(G+e) if j is exceptional and
1/(G-e) otherwise. In the latter case choose its one minor feature to equal the
query feature. Thus all exception patterns give distinct exact answer vectors.
Any self-contained summary answering every admitted query must distinguish all
binom(G,e) cases: a fixed-length summary needs at least ceil(log2 binom(G,e))
bits. The same statement for arbitrary variable-length encodings has the usual
length/decodability qualification. For e<=G/2 this is Omega(e log(G/e)).

This does not establish an Omega(e)-machine-word bound, a query-time lower bound,
or optimality of our sorted-key representation. It also does not apply when an
external per-leaf side table or raw row bodies are available for free, or when
queries may be refused/approximated. The construction prevents calling r alone
a sufficient parameter for unrestricted exact reusable summaries in this model.

The lead checker distinguishes all 172 profiles for G=4,8 and 1<=e<=G/2, through
1,336 singleton queries. It also asserts identical core nodes within each
(G,e) group. This finite check supports, rather than replaces, the counting proof.

## 8. Prior Art And Contribution Boundary

| Primary source inspected | Imported foundation and remaining distinction |
| --- | --- |
| [Zhang et al., Transformation-based KNN Set Similarity Search](https://www.jinwang18.net/files/tkde19-setknn.pdf), Sections 3-5, p.5 Lemma 2 and p.15 Appendix C | Group-count vectors and query/node Jaccard bounds are established. Its coordinate-box bound does not supply this exact jointly attained two-row summary theorem. The count-vector filtering idea is not new here. |
| [Raman, Raman, Rao, Succinct Indexable Dictionaries](https://arxiv.org/pdf/0705.0552), pp.2-3, p.14 Theorem 4.1 | Subset counting lower bounds and succinct membership/rank/select dictionaries are established. Our Section 7 maps exact envelope answers to those subset patterns; it is not a new information-theoretic technique. Rank on a present key gives the exception payload index; absent keys use the regular rule. |
| [Closest-Art Assessment](Similarity-Orientation-Prior-Art-Assessment.md), inspected primary sources S1-S4 | Discrete concave allocation, branching on a supplied small conflict set, and removal of redundant laminar quotas remain credited. The exception representation inherits rather than replaces the model-specific orientation and companion-realization proofs. |

This bounded literature check adds the dictionary precedent and rereads the
count-vector bound; it does not establish that no published paper contains the
whole result. The candidate contribution is the combined structural/query-domain
reduction with an exactness proof, collapse theorem, and model-specific necessary
information example. Whether that is substantial enough for a paper remains
open. An equal-information control allowed the same core and exception profile
can implement the same bounds; that is not itself evidence of prior publication.

## 9. Reproducible Implementation Evidence

Files: [probe](experiments/probe_exception_core_similarity.py),
[tests](experiments/test_exception_core_similarity.py). New public APIs are
`compile_exception_capacity_core`, `prepare_exception_query_core`, and
`compute_exception_core_bound`. Existing helper modules were not edited.

The initial twelve tests failed with the intended missing-module assertions
before implementation. The floor-only plan was abandoned before implementation,
not labeled a successful feature. The new exception-capacity-saturation test then
failed because it retained a full 10^100 value instead of 3; clipping fixed that
stored-field assertion without changing any admitted answer.

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest test_exception_core_similarity -q
```

Lead execution after clipping: 14 tests passed in 0.616 seconds. This is test
runtime, not a benchmark. Coverage includes:

- 58,880 complete five-feature metadata/query/profile cases: 52,636 admitted
  exact maxima and 6,244 correctly refused out-of-profile cases.
- 192 independently generated prepared-core parity cases and original-core scores.
- 139 strict-node patterns, including nested/adjacent splits; sharp 4/7-run cases.
- Six G-scaling families, nine mixed regular/exception queries, 552 collapse
  examples and the unsafe aggregate-floor counterexample.
- 172 information profiles/1,336 singleton queries; huge clipped counts.
- Pre-stream work refusal; malformed/empty inputs; immutable state and successful
  input release with cyclic GC disabled; unchanged state after iterator failure.

The full similarity suite subsequently passed **103 tests in 23.082 seconds**
using `-m unittest discover -p 'test*similarity.py' -q` in the same directory.
This is a regression result, not a public-query performance comparison.

The lead also read and replayed the independent review's retained checker,
exit zero. Separate evidence includes 64,979 structural masks over 626 ordered
tree shapes, 306 exception envelopes, 12 balanced-family feature witnesses,
3,514 singleton queries distinguishing 494 exception patterns, and 2,025
clipping identities. All 66 feasible floor-profile envelopes equal the ordinary
union/length bound; two infeasible cases are retained. These are finite checks,
not counts of distinct production datasets. Review SHA-256 at replay was
`6f2029ab6b96719ba96555314c10daeb2abb795e390d0fcce9bc305c6bc7e184`.

Artifact SHA-256 at this checkpoint:

```text
probe fd8b378f2dee0dddc329397ee462ca6c898526fdcd6285354184bbb23aa56b6a
tests d28d3ba785da1ab0d8454432231d8e80815c7e2bfd7b318a82115e134dde3fe3
```

## 10. Product Interpretation And Next Scientific Gate

The useful question is whether real target-pair summaries have **both** small r
and small e(tau) for queries customers actually issue, after paying for sparse
query feature mapping. A small e obtained by refusing almost every useful query
is not a product win. Nor does an exact cheaper bound help if ordinary interval
filters or direct posting merge finish sooner.

The next meaningful comparison is paid preparation plus full-query execution
against the existing static core, interval control, union control and DAAT on
identical inputs, with a declared tau/refusal policy and r/e histograms. Preserve
the earlier negative complete-query receipts; this implementation does not alter
their 10.32%/13.81% penalties. No new public timing was run for this increment.

Refresh must publish counts, ownership, exceptions and source-body identities
together. A change can turn a regular leaf exceptional or create many strict
nodes; the old profile cannot silently survive it. Build peak, source extraction,
all retained families, pinned old versions and output still count toward the
whole-workflow budget. This result removes one previously explicit O(G) retained
table under a declared domain; it does not prove arbitrary-graph 4 GB operation.

## 11. Stronger Modal Query Signatures

The fixed profile assumes every unlisted leaf has query rewards (q,q). That is
only one possible regularity. A component can instead contain many leaves with
the same lower occupancy and capped capacity, including empty leaves. Calling
every one of these leaves exceptional can retain unnecessary records.

Define the saturated signature

```text
sigma_tau(j) = (min(l_j,tau), min(C_j,tau)).
```

Store one most frequent signature per leaf-owning additive component, with
lexicographically smallest signature breaking ties. Store (leaf,signature) only
where a leaf differs from its component default. Strict nodes and components
without ordinary leaves have no used default. Ownership still comes from the
same at-most-3r+1 runs. Let d count these deviations.

### Exact Query-Behavior Equivalence

For every admitted q<=tau, the signature gives the same two minimum rewards as
the original (l,C). It also determines validity q<=l+C: if l or C reaches tau,
the clipped sum is at least tau and all admitted q pass; if neither reaches
tau, the sum is unchanged. Therefore default/deviation lookup reconstructs the
exact old reward and population predicate, not an approximate histogram.

There is also a converse. Regard a leaf's restricted behavior as the valid/
invalid result and, when valid, both minimum rewards for each integer 0<=q<=tau.
Let m be its greatest valid query count. If m=tau, the two rewards at tau recover
the saturated signature. If m<tau, then H=m and both l,C<=m; the rewards at m
recover the original l,C. For tau=0 every signature and behavior is zero.
Thus **two leaves have identical restricted local behavior exactly when their
saturated signatures agree**. This is a coarsest local-behavior representation,
not a proof that no further compression of the final envelope is possible.

The same component-reward and global companion-realization argument then proves
the full envelope unchanged. Query execution resolves a deviation before testing
the default population: otherwise a common empty default would incorrectly
reject a valid positive query at a nonempty exceptional leaf.

### Bound And Preparation Tradeoff

The fixed signature (tau,tau) occurs at every regular leaf of the previous
profile. Choosing a mode in each component retains at least as many default
leaves as choosing that fixed signature. Consequently d<=e. This minimizes
deviation **count** among one-default-per-component exact-signature schemes;
it does not minimize bytes, preprocessing time, decoder cost or full query time.
Two additional default scalars per component can outweigh a tiny reduction in
deviation records on a small tree.

The implementation is `compile_modal_capacity_core` in the existing exception
probe. It first pays the fixed-profile compilation, constructs G
(owner,clipped_lower,clipped_capacity) records, sorts them deterministically,
counts equal runs, and chooses each mode. A final original-leaf traversal emits
deviations in leaf-ID order. This is O(G log G) comparison work and O(G)
temporary records, not an unqualified linear hash-histogram guarantee. Original
full core counts are not clipped. The resulting core and defaults are immutable;
only query-local bundles are changed during evaluation.

The existing query procedure now accepts either compiler output. Its optional
`component_defaults` tuple changes only the source of the two reward caps.
In the modal variant, `exceptions` and `exception_keys` name d **deviations**,
which need not be the l<tau leaves counted by e. Sparse query cost is
O(r+s log(d+2)+(r+1)2^r), with O(r+1) extra records and O(r+d+1) retained records.
Raw feature mapping and the exponential orientation budget remain unchanged.

### Why Zero Deviations Need Not Collapse

For an additive tree with G>=8 leaves, set C_j=3,H_j=4 everywhere, u=3G,
ell=G, tau=4. The fixed scheme retains e=G exceptions; the modal scheme has r=0,
d=0 and one default (1,3). Query all four union features in one leaf, a=4.
The major row overlaps three; the minor row is forced to its lower occupancy
one in every leaf. The exact envelope is 3/(3G+1), whereas union/size alone
allows 4/G. Both row systems are feasible. The gain comes from knowing the
nontrivial default, not from ignoring all leaf constraints.

The balanced one-strict-node family in Section 6 likewise has d=0: ordinary
leaves share (2,2), and each exceptional singleton component owns its own
(1,2) default. Its envelope remains 3/(u+1). A different sparse-empty fixture
uses (0,0) as the default and stores only the two nonempty leaf positions.

The position-information lower bound is not contradicted. Its two signatures
still encode an arbitrary subset; a modal encoding may store the complement
when cheaper. The binomial information remains necessary. Large r, many distinct
signatures or raw query preparation can still dominate the entire workflow.

### Attribution And Evidence

[Baunsgaard and Boehm, AWARE (2023)](https://mboehm7.github.io/resources/sigmod2023a.pdf),
Section 3.1, pp. 2:4-2:5, explicitly uses a most-frequent tuple default and stores
locations of non-default tuples. That is direct prior art for the coding step.
Here the additional argument is that a particular capped pair captures exactly
the restricted leaf behavior and composes with the reviewed attained-capacity
core. No new generic compression primitive or established scientific priority
is claimed.

Five missing-compiler assertions preceded implementation. The sixth test checks
the local-behavior equivalence without requiring a new implementation operation.
The [six modal tests](experiments/test_modal_core_similarity.py) and fourteen
fixed-profile tests pass together: 20 tests in 0.660 seconds, including 320
whole-core/score comparisons, 567 single-leaf admission cases and 405 local
behavior/signature cases. The six uniform G-scaling examples retain no
deviations; the strong r=1 family and empty-default counterexample also pass.
These are finite arithmetic tests, not timings on customer workloads.

Current post-modal artifact SHA-256 values supersede the probe checkpoint in
Section 9, whose test receipt remains historical:

```text
probe       6dfabd7095abc79786fc621b53d1c1ea6c8d5d354b2b271d674f4ce2b6d79783
modal tests a5400680f18d2ad25ff3e467adff85667bd67476c7a76fb7a389cc172ab3d548
```

The earlier mathematical review covers fixed exceptions and clipping, not this
later implementation. The completed [workload sidecar](Similarity-Exception-Workload-Profile.md)
independently supports modal reward/validity preservation, d<=e, and the local
behavior-equivalence theorem. Its retained checker covers 594 local profiles,
2,970 query points and 19,899 profile-pair comparisons. The lead replayed that
checker together with the complete bounded workload diagnostic. Neither review
is an independent audit of the modal implementation.

The post-modal full suite passed 109 tests in 25.663 seconds. The subsequent
compressed-dual addition passes 116 tests in 25.823 seconds; no modal source
change intervened. These are verification durations, not performance results.

## 12. Public Evidence Changes The Next Step

The workload diagnostic preserves the original GrQc/Facebook data, normalization,
physical adjacent-ID pairing, 30 frozen sources per dataset, and modulo feature
groups. It evaluates exact bounds only at G=4; G=16,64 are counts-only
sensitivity profiles. It does not run a new top-k schedule or measure RSS/time.

| Finding | GrQc | Facebook | Interpretation |
| --- | ---: | ---: | --- |
| G=4,tau=8 admitted positive nonself calls | 1314 | 11228 | 12542 distinct exact bound evaluations in total |
| Of those, exact bound tighter than existing interval | 59 | 46 | 105 logical opportunities, not measured pruning events |
| G=64,tau=8 mean fixed exception e | 64 | 64 | Fixed-default profile fails to shrink the leaf table on every full pair |
| Same mean modal d | 8.790 | 27.847 | Fewer deviations, but owner/default/index bytes still count |
| Full pairs with charged entry proxy R+d+J<64 | 2613/2621 | 1353/2019 | Conditional representation evidence, not physical memory savings |
| G=64 r median/p90/maximum | 1/4/17 | 5/13/28 | Larger G makes unrestricted orientation enumeration unsafe |

J is the number of leaf-owning component defaults; the proxy charges those
separately. Its entries are not equal-sized records. Query-weighted profiles
differ from static pair averages and are retained in the diagnostic. Sparse
query support does not by itself bound numeric overlap Q.

The lead replay reproduced all six dataset/G profiles, frozen posting totals,
12,542 exact scores and the independent signature checks with exit zero. The
source graph is resident in this diagnostic. There is no low-RAM ingestion or
complete-workflow experiment here.

The resulting [compressed query-dual evaluator](Similarity-Compressed-Dual-Core.md)
addresses the high-r computational obstacle without restoring G-sized query
tables. It applies the prior inverse-threshold recurrence to generalized
component bundles. Its seven new tests pass, including exact oracle envelopes,
a 128-conflict case and a huge-Q refusal that the branching route can handle.
This changes the available evaluation regimes, not the exact envelope or the
old negative public timing results. The completed independent theorem challenge
accepts the composition; the lead replayed its separate 8,050-root-envelope
and 22,730-subtree-threshold checker. It is not a code audit or speed result.

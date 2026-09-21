# Similarity: Query-Induced Feasibility Skeletons

Date: 2026-09-21. A05 parameter refinement with finite implementation and
public opportunity profile; independent mathematical challenge completed. No physical RAM
or priority claim.

## Why This Supersedes A Mechanical Refactor

The compressed interval control now removes dense query metadata, but all
exact schedules still copy/evaluate the entire K-node conflict core. A sparse
query can intersect only a few component bundles while most strict subtrees
have zero query reward. Sharing that full preparation avoids duplication;
eliminating irrelevant evaluation state can give a stronger bound.

Three choices: share the existing K-record core; memoize zero-query states but
still scan K records; or compile neutral subtree feasibility and visit only
the ancestor closure of queried components. Investigate the third, retaining
the first two as honest controls. This is authorized proof/prototype work
within the ongoing goal, not a new product scope or permission to commit.

The expert lenses are exact pair realizability, sparse incremental evaluation,
query-engine state accounting, and adversarial counterexamples. Existing
negative public timings and the seven-family obligations remain unchanged.

## Inherited Model And Feasibility

There are exactly two hidden rows. Each feature-tree subtree has union H_v
and attained maximum row size C_v. With shared intersection e, its other row
has size H_v+e-C_v. Earlier reviewed work already proves that feasible shared
counts form an interval [E_v, 2C_v-H_v], symmetric between the scored-row
roles. The new proposal does NOT invent this feasibility interval theorem.

Precompute E_v on the static compressed core with the all-zero query. For
an ordinary additive bundle, E=0. At an additive component, E is the sum of
its strict-boundary children's E values. At a strict node with children L,R:

```text
D = C_L+C_R-C_v > 0
X = 2*C_L-H_L-D
Y = 2*C_R-H_R-D

require E_L <= X and E_R <= Y
E_v = min(Y+E_L, X+E_R).
```

Reject an infeasible subtree or a root size with ell+C_root-H_root<E_root.
The scalar recurrence is the zero-reward instance of the existing inverse DP.
Static parent links, populations and E values add O(K) records, paid once.

## Neutral Subtree Lemma

If a subtree contains no query elements, its two row roles have reward zero
for every feasible size. It can therefore be replaced by a neutral capsule:

```text
major row size: C
minor row size: any integer in [H-C+E, C]
query reward:  0 for either role
```

Existence for every integer in that range and role symmetry are exactly the
inherited feasibility theorem. The capsule still carries actual H. Its lower
bound plus upper bound is H+E, NOT H. Inferring population from this shortened
domain would be false when E>0. Do not replace it by an ordinary physical leaf.

At an additive component, all inactive boundary subtrees have their maxima on
the component-major row. Combine their C,H,E values by addition. For an active
component's ordinary leaves, retain the prior query bundle (L,U,A,B), and put
b=L-A. Add inactive totals C0,H0,E0 to get one offset bundle:

```text
capacity   Cb = U+C0
population Hb = L+U+H0
floor      lb = Hb-Cb+E0
reward     f(x) = min(x-(lb-b), B), lb <= x <= Cb
```

The component-major reward is B. The component-minor reward formula follows
by giving the inactive region its minimum feasible intersection E0 before
allocating additional intersection to rewarding ordinary leaves. Extra
intersection can subsequently be distributed without holes; B-b<=U-L
comes from the original leaf allocation lemma. For inverse evaluation the
bundle thresholds are E0 for the major role and E0+max(0,c-b) for the minor.
This offset, and actual Hb, are essential new integration obligations.

## Query-Induced Construction

Let s be the number of positive query leaves. Use binary search over existing
owner-run endpoints and deviation keys to update only the named component
bundles. Mark those owners and their ancestors using paid parent links,
stopping at already marked nodes. Record active child edges while marking.
Call the resulting ancestor-closed set A, its size Kq, and its strict-node
count rq. This is ordinary sparse dependency propagation, not a new tree
primitive. No original strict node on an active path may be skipped.

At a marked strict node, keep both ordered child positions. A missing child
becomes a neutral capsule. At a marked additive component, subtract its active
children's C,H,E from the precomputed boundary totals, and merge all remaining
children into its offset bundle. Iterate active edges only: scanning a large
component's full child list would destroy the claimed query bound.

The reduced orientation core has Ks<=Kq+rq records. All retained strict bits
are renumbered; there are rq, not r. Old orientation labeling and conditioned
four-scalar row optimization can evaluate this reduced core unchanged, provided
population is carried separately and offset floors are not erased. The sparse
preparation is performed once. A zero-overlap query uses one neutral root.

## Correctness And Resource Claim

Neutral capsules preserve the full feasible boundary-size domain, not merely
one score. Offset bundles preserve the optimal scored-row reward for each
feasible aggregate boundary size. Strict nodes retain their original attained
maximum disjunction. Induction by substitution therefore preserves the exact
maximum Jaccard score over ALL complete pairs matching the original summary.
It does not reconstruct the original rows or jointly maximize both rewards.

For the existing balanced G-leaf compiler, every core parent edge descends the
original feature tree. Hence Kq<=min(K,s*(log2(G)+1)) for s>0. Let R be owner
run count and d the number of deviations. With already supplied sorted leaf
counts, query construction takes

```text
O(1+s*(log(R+1)+log(d+2)) + Kq) arithmetic/index work
O(1+Kq+s) transient records before dropping sparse owner updates
O(K+d+R) paid static records, plus original graph/posting storage
```

This construction count assumes unit-cost dictionary operations. The Python
probe uses integer-keyed dictionaries/sets, not a worst-case real-time mapping
structure. A deterministic balanced-map implementation pays additional lookup
log factors; physical memory and bit costs are separate from either model.

The branching evaluator uses O(Ks*2^rq) counted work and O(Ks) row/label
records. Its reservation must precede branch enumeration, but query validation
and skeleton construction have already been paid. An inverse evaluator would
use the offset thresholds above; that integration is not initially implemented.
No O(s)-node virtual tree claim is made: unary strict ancestors remain.
If most components are queried, Kq approaches K and there may be no benefit.

## Two Critical Falsification Cases

1. A root additive component has one queried singleton and one inactive strict
   subtree with leaf populations/capacities (2,2), parent C=3,H=4,E=1.
   Root C=4,H=5,ell=2 and a one-element query give exact envelope 1/4.
   Dropping inactive E falsely allows the minor row to use that query element,
   returning 1/2. Thus inactive does not mean unconstrained or free.
2. Put one strict split under each sibling leaf pair, all higher splits
   additive, with H_leaf=4,C_leaf=3,C_pair=4. A query touching only the first
   pair has rq=1 even as global r=G/2 grows. The exact envelope remains
   3/(2G+1) for two query elements per first-pair leaf and equal root row sizes.
   The reduced branch solver should use two orientations; the old whole-core
   branch solver must refuse beyond its cap. Compare the polynomial full-core
   dual too, not merely the exponential solver.

## Implementation And Review Plan

Add `experiments/test_query_skeleton_similarity.py` first and observe its
missing-module assertion. Implement `experiments/probe_query_skeleton_similarity.py`
without changing any frozen modules/receipts. Reuse the existing orientation
evaluator only after constructing the correct offset bundles.

Verify complete small compatible-pair envelopes, actual deeper summaries,
empty/invalid queries and root feasibility, the nonzero-E counterexample,
fixed local conflict versus growing global conflicts, exact branch budgets,
and a guard forbidding scans of inactive boundary children. Independently
challenge the substitution proof while the lead implements the finite probe.
The reviewer owns only its separate Markdown file. Do not run timings here.

## Scientific Boundary

The [earlier feasibility review](Similarity-Overlap-Frontier-Review.md#3-feasible-domain-theorem)
and [compressed dual](Similarity-Compressed-Dual-Core.md) are direct local
prerequisites. Cached dependency propagation and partial evaluation are known.
[Koch, Lupei and Tannen](https://arxiv.org/abs/1412.4320) give explicit cost
analysis and semantics-preserving incrementalization in a different collection
query setting; the inspected abstract does not establish this exact reduction.
[Kumabe, Maehara and Sin'ya](https://arxiv.org/abs/1807.04942) provide prior
tree-DP improvements; generic DP/tree compression cannot be our novelty claim.

The candidate contribution is the complete-pair neutral/offset substitution
and resulting query-local conflict parameter on a compact static signature.
Historical priority, useful public workload separation, packed representation
and whole-process RAM remain separate gates. This is not seven finished papers.

## Implemented Evidence

The [probe](experiments/probe_query_skeleton_similarity.py) was implemented
after the first explicit missing-module assertion failed. Its [six tests](experiments/test_query_skeleton_similarity.py)
pass 29,440 complete compatible-pair/query envelopes on the existing finite
corpus, plus 180 seeded deeper references with four-feature queries. Reusing
that old corpus is not discovering 29,440 new datasets. Other cases exercise
positive neutral E, single-pass inputs, empty queries, invalid values/root
sizes, branch/work/skeleton refusal and a guard which prohibits iterating
inactive boundary children at a high-degree additive component.

The full similarity suite then passed **138 tests** with warnings as errors,
terminal exit zero. Its 25.013 seconds is test execution cost, not a benchmark.
The whole-core dual below was actually evaluated and returns the same score;
its cells and recurrence steps are not the same units as branch row records
and branch-work counters.

| G | Global r | Static K | Local rq | Reduced Ks | Local orientations | Local counted work | Whole-core dual cells | Whole-core dual work |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 2 | 7 | 1 | 4 | 2 | 48 | 50 | 88 |
| 16 | 8 | 25 | 1 | 4 | 2 | 48 | 146 | 220 |
| 64 | 32 | 97 | 1 | 4 | 2 | 48 | 530 | 748 |
| 256 | 128 | 385 | 1 | 4 | 2 | 48 | 2,066 | 2,860 |

The static price is explicit: three new K-entry arrays (parent, population,
minimum shared count) and R owner-run endpoints, in addition to the old modal
summary and graph/index storage. In this family R=G. The prototype's current
`row_record_slots_peak` means one evaluation table's slots, not all live root
tuples, labels, allocator objects or process memory. Preparation uses mutable
maps, a traversal stack and output records; the branch cap does not include
their work. A skeleton refusal may occur after consuming part of the sparse
stream. No refused branch enumeration is executed.

This removes a real global-r dependence for the declared query-local regime.
An equally cached generic two-row DP can exploit neutral states too; the fair
next comparison must include that adaptation. The core issue is the exact
neutral/offset substitution and compact static-query interface, not claiming
that visiting only changed dependencies is a new general technique.

Source snapshot SHA-256:

- Probe: `38cb3ea1021c2b0821d4e731e69a9c1384be2698dc13820cc1650ac8708b73b4`.
- Tests: `c77e2878e1e0dc4845d512887113ff4062c978529e0d971a519ee8c0deb147b8`.

## Fixed Public Opportunity Profile

The following resident profiler reuses the two frozen sources and thirty source
IDs each, at G=64 only. It measures all positive relevant blocks BEFORE the
top-k witness filters. Therefore its newly admissible calls are an opportunity
upper bound, not calls that an actual top-k executor necessarily needs. It
constructs skeletons and compares reservations; it does not execute branch
enumerations, measure time/RAM, or claim new query-output verification.

The unclipped tau profile matches the sparse-control study: total source
adjacency memberships. Original-ID pairing, query semantics and modulo grouping
are unchanged. Construction retains the old resident objects for this profiler.

```python
from collections import Counter
import gzip
from heapq import merge
from itertools import groupby
import json
from pathlib import Path
import sys

root = Path('research_algorithms_20260920')
sys.path.insert(0, str(root / 'experiments'))
from profile_large_group_similarity import prepare_large_group_source, hash_similarity_input_file
from bench_bounded_seed_similarity import read_undirected_graph_source
from probe_query_skeleton_similarity import compile_query_skeleton_index, prepare_query_skeleton_core

prior_path = root / 'evidence/similarity-large-groups-20260921/receipt.json'
assert hash_similarity_input_file(prior_path) == 'bf50ab091409266c153f82291b9001a2bcd0c1ec447a9d0faf074397f8d0796a'
prior = json.loads(prior_path.read_text())
for item in prior['inputs']:
    assert hash_similarity_input_file(item['source']) == item['sha256']
    with gzip.open(item['source'], 'rt') as stream:
        graph, info = read_undirected_graph_source(stream)
    assert info == item['normalization']
    source = prepare_large_group_source(sorted(graph.items()), 64, query_limit=info['memberships'])
    indexes = [compile_query_skeleton_index(block['core']) if block['core'] else None
               for block in source['blocks']]
    paid = Counter()
    for sid in item['source_ids']:
        query = graph[sid]
        def emit_indexed_posting_records(feature):
            for bid in source['block_posts'].get(feature, ()):
                yield bid, feature % 64
        streams = [emit_indexed_posting_records(feature) for feature in sorted(query)]
        for bid, occurrences in groupby(merge(*streams), key=lambda pair: pair[0]):
            pairs = [(leaf, sum(1 for _ in repeated))
                     for leaf, repeated in groupby(occurrences, key=lambda pair: pair[1])]
            paid['positive_blocks'] += 1
            paid['posting_occurrences'] += sum(count for _, count in pairs)
            block, index = source['blocks'][bid], indexes[bid]
            if index is None:
                paid['singletons'] += 1
                continue
            metrics = {}
            reduced = prepare_query_skeleton_core(index, len(query), block['minimum'], pairs, stats=metrics)
            old = block['core']
            old_work = (1 << old['strict_nodes']) * (5*len(old['nodes']) - 2 + 2*old['component_nodes'])
            new_work = (1 << reduced['strict_nodes']) * (5*len(reduced['nodes']) - 2 + 2*reduced['component_nodes'])
            old_ok = old_work <= 65536 and (1 << old['strict_nodes']) <= 65536
            new_ok = new_work <= 65536 and (1 << reduced['strict_nodes']) <= 65536
            paid.update(full_pairs=1, old_core_records=len(old['nodes']), new_core_records=len(reduced['nodes']),
                        old_strict_sum=old['strict_nodes'], local_strict_sum=reduced['strict_nodes'],
                        smaller_skeleton=int(len(reduced['nodes']) < len(old['nodes'])),
                        fewer_conflicts=int(reduced['strict_nodes'] < old['strict_nodes']),
                        old_branch_admitted=int(old_ok), local_branch_admitted=int(new_ok),
                        newly_branch_admitted=int(new_ok and not old_ok))
    controls = [row for row in prior['runs'] if row['dataset']==item['dataset']
                and row['groups']==64 and row['mode']=='interval']
    assert paid['positive_blocks'] == sum(row['metrics'].get('blocks_seen', 0) for row in controls)
    assert paid['posting_occurrences'] == sum(row['metrics'].get('block_memberships', 0) for row in controls)
    print(json.dumps(dict(dataset=item['dataset'], G=64, counts=dict(paid),
        static_index_nodes=sum(len(idx['parents']) for idx in indexes if idx),
        static_owner_endpoints=sum(len(idx['owner_ends']) for idx in indexes if idx)), sort_keys=True))
```

Executed once from the retained listing, terminal exit zero:

| G=64 profile | GrQc | Facebook |
| --- | ---: | ---: |
| Positive full-pair calls, including self-containing blocks | 1,436 | 12,736 |
| Expanded relevant-block posting occurrences | 7,237 | 155,786 |
| Calls with fewer conflicts and a smaller skeleton | 924 | 10,023 |
| Sum of original core records over calls | 11,322 | 250,669 |
| Sum of reduced records over calls | 5,460 | 89,552 |
| Sum of original strict-node counts | 3,426 | 83,988 |
| Sum of query-local strict-node counts | 1,432 | 27,514 |
| Whole-core branch reservations within cap | 1,415 | 8,697 |
| Local-core branch reservations within cap | 1,434 | 11,922 |
| Newly admitted branch reservations | 19 | 3,225 |
| K entries in each added static array | 16,702 | 37,507 |
| Additional static owner endpoints | 13,374 | 28,871 |

No positive singleton occurs in these fixed queries. The posting/call totals
match the old full-query receipt. Record sums decrease by about 51.8% and
64.3%, respectively; these are aggregate kernel records before witness filters,
NOT peak RAM reductions. Branch admission uses at most 65,536 orientations and
65,536 reserved branch-work units. The local method still refuses 2 GrQc and
814 Facebook candidate calls under that reservation. A full-core dual may
already admit many of the newly branch-admitted cases; this table does not
claim a new exact-query capability over that polynomial control.

## Independent Challenge Completed

The [separate mathematical review](Similarity-Query-Induced-Review.md) accepts
the reduction with the nonzero-E, actual-population and scan-free requirements.
The lead read and replayed its standalone, project-import-free checker:
2,657 feature-pair frontier comparisons, 26,858 root-envelope comparisons,
four invalid summaries, two explicit offsets and eight scaling fixtures pass.
This is bounded independent mathematical evidence, not a production code audit.
The reviewer is completed and closed. The full 138-test run reported above
predates this documentation-only integration; no frozen probe changed.

The review adds the structural bound Ks<=3*rq+1 and makes the uniform empty
query +1 cost explicit. For arbitrary tree height h, use Kq<=s*(h+1), not
log2(G). Owner endpoints are charged explicitly as R here. A similarly
equipped memoized tree DP can attain the same bound; publication priority is
unresolved. The specialized substitution is not a new caching primitive.

## Next Gate

The [shared query-local executor](Similarity-Query-Local-Execution.md) now
implements offset-neutral inverse bases and a separately named neutral-aware
cheap control. Its independent code review is integrated, including a fixed
plan-lifetime defect and the illustrative-pair erratum. All 146 similarity
tests and 300 public complete outputs pass, with 120 affected outputs/counters
revalidated after the lifetime fix. Actual post-filter work improves modestly;
peak threshold cells do not. This supersedes the larger pre-filter opportunity
ratios as practical evidence. Require useful workload or closest-art theorem
separation before a packed full-ingestion implementation; retain DAAT and all
static/build/output costs.

# Static Core With Sparse Query Updates

Date: 2026-09-21. Bounded A05 count-only follow-up. Implemented; 16 new tests,
29 focused tests, and 89 full similarity tests pass. No production integration,
public-workload timings, core novelty, low whole-RAM, or graph-query-engine claim.

## Approved Design And Plan

The supplied design separates paid static compilation from per-query rewards.
Use the existing additive-region compiler with all-zero query counts; retain
immutable topology, capacities, component L/U and a paid O(G) indexed table of
`(component owner, H-C, C)` per original leaf. Sorted unique positive sparse query
pairs update only cloned core bundles. Reuse the existing canonical-label and
conditioned-row helpers without editing any existing module.

1. Completed: add parity, streamed-input, validation, admission, and ownership
   tests; observe 16 missing-module failures before implementation.
2. Completed: implement immutable static compilation and sparse rewards; factor
   logical branch/work reservations before enumeration, with no fallback.
3. Completed: verify exact original/current-core parity and structural access
   counters; record commands, limits, proof obligations, and artifact hashes.

Only this note, `experiments/probe_static_core_similarity.py`, and
`experiments/test_static_core_similarity.py` are owned. No commit or push.

## Contract And Existing API

The basis is [Additive-Region Core](Similarity-Additive-Region-Core.md), especially
Sections 4-7 and 10, and the boundary-projection and optimal-lifting arguments in
[Additive-Core Review](Similarity-Additive-Core-Review.md), Sections 1-6. This
follow-up reuses that reduction; it does not establish a new core theorem or
scientific priority. The old modules and their tests are unchanged.

The exact existing APIs used, read directly from the current implementation:

```python
build_additive_orientation_core(capacities, counts, union_counts, *, stats=None)
orient_additive_core_labels(core, assignment, *, stats=None)
evaluate_additive_core_records(core, labels, row, *, stats=None)
```

The compiler returns a dictionary with `nodes`, `root`, `strict_nodes`,
`component_nodes`, `union_size`, `query_union_size`, and `major_size`. Postorder
nodes contain `original_node`, `capacity`, `children`, `strict_bit`, and `bundle`.
A component bundle is `(L,U,A,B)`; a strict node's bundle is `None`. The row helper
returns a postorder table of capped-linear records or `None` for infeasibility.
It must not be treated as a helper returning only a root score.

The new [probe](experiments/probe_static_core_similarity.py) exposes:

```python
compile_static_capacity_core(capacities, union_counts, *, stats=None)
prepare_static_query_core(static_core, a, leaf_queries, *, stats=None)
compute_static_core_bound(static_core, a, minimum, leaf_queries, *,
                          branch_cap=1000000, work_cap=10000000, stats=None)
```

- Static inputs are repeatable count sequences in the existing balanced heap
  representation: power-of-two G in 1..256, `len(capacities)==2*G`, zero sentinel
  at index zero, leaf populations in `union_counts`. Do not mutate during compile.
- Existing validation is executed, not disabled: exact built-in nonnegative
  integer metadata, `C<=H<=2*C`, parent at least each child maximum, and parent
  at most their sum. Boolean, float, and integer-subclass coercions are not used.
- The outer query input may be a single-pass iterator or generator. Each entry
  is a two-item tuple/list `(leaf_id,q)`, using zero-based original leaf indices.
  IDs must be strictly increasing, unique built-in integers in `[0,G)`; q must
  be a positive built-in integer with `q<=H`. Omitted leaves mean q=0.
- `a`, `minimum`, and budgets must be nonnegative built-in integers. The query
  enforces `sum(q)<=a`, `minimum<=u`, and `minimum+u>=H_root`, with u=C_root.
  Outside-union query population is retained through a, not added to any leaf.
- Return an exact `Fraction` for the maximum score of either row across complete
  compatible pairs. Zero overlap has score zero, including empty/empty. No
  feasible complete pair raises `ValueError`; it does not return a loose bound.
- Static cores are trusted compiler outputs, not a deserialization boundary.
  Forged/replaced records are unsupported. Tests substitute a read-only indexed
  table guard solely to measure accesses. No eval, pickle, or validation bypass.

## Static Ownership And Sparse Updates

The existing compiler runs exactly once, with G zero query counts. Each component
then has `(L,U,L,0)`. A separate paid original-tree traversal assigns each leaf to
its component and stores `(owner,H-C,C)` in a G-entry indexed tuple. Component
tops are indexed by original node; crossing into a new non-strict component
replaces the inherited owner. Strict nodes are never leaf owners. Adjacent strict
nodes and zero-leaf components are retained unchanged.

Top-level and node dictionaries are exposed through `MappingProxyType`; all
children, bundles, and leaf records are tuples of immutable integers. No mutable
input sequence is retained. Successful compilation releases input arrays even
with cyclic GC disabled, using the current helper's existing cycle fix. Caller
mutation after compilation cannot alter capacities or the leaf table.

For each query, copy only the K core dictionaries, sharing immutable topology
tuples. Read one `(owner,l,c)` table entry for each supplied pair and replace that
owner's query-local bundle as follows:

```text
initial component: (L,U,A,B) = (L,U,L,0)
one sparse leaf:   A <- A-min(q,l)       B <- B+min(q,c)
final component:   A = L-sum_j min(q_j,H_j-C_j)
                  B = sum_j min(q_j,C_j)
```

No sorting, duplicate set, query list, dense q array, full leaf-table scan, or
original-tree revisit occurs in query preparation. Only a previous ID and running
sum are needed alongside the copied core and current pair. Prepared results do
not retain the leaf table or the input stream. The helper prepares rewards only;
row-size admission and branch budgets belong to `compute_static_core_bound`.

Each update touches a new query's dictionaries, never the static records. Partial
validation failure, generator exceptions, budget refusal, and an infeasible
envelope cannot change static state. Source exceptions propagate; no fallback
score is substituted. Retained exception tracebacks or caller-owned iterator
buffers can of course retain objects; they are not claimed away.

## Preservation Argument

The static compiler establishes exactly the same topology, capacities, branch
bits, root metadata, and L/U as query-dependent compilation: those fields depend
only on capacities and union populations. Its only query-dependent fields are
bundle A/B and `query_union_size`.

For every omitted leaf, both minimum terms are zero. For every supplied leaf,
the indexed record provides its exact unique owner and its original H-C and C.
Strict ordering excludes double-counting. Therefore the final A/B and query sum
equal the dense query-dependent compiler field-for-field. An untouched component
correctly keeps `(L,U,L,0)`, including zero bundles with strict children. The
positive-A counterexample remains `(2,4,1,2)`, not an aggregate physical leaf.

The evaluator calls the existing canonical orientation and row-frontier helpers.
Both prescribed root sizes must lie in their feasible domains before scoring
either row. Thus impossible companions remain rejected, ties add no orientation
bit, and the original boundary/optimal-lifting proof applies unchanged. Exact
Jaccard uses `overlap/(a+row_size-overlap)` and retains outside-union features.
Different marginal optima may still require different compatible companion sets.
No suboptimal-overlap spectrum, joint optimum, or feature witness is promised.

## Paid Costs And Admission

Let r be the number of strict nodes, c the number of additive components,
K=r+c<=3r+1, and E=K-1. The static object includes both its O(r+1) core and the
**paid O(G) resident leaf table**. It is not a low-whole-RAM implementation.

| Phase | Arithmetic work | Live count-word allowance |
| --- | --- | --- |
| Validate and compile with zero q | O(G) | O(G), including transient inputs/arrays |
| Assign leaf ownership and freeze | O(G) | O(G) |
| Retained static object | No per-query rebuilding | O(G+r+1), including all G leaf records |
| Query admission | Scalar factorization, no assignment enumeration | O(r+1) including assignment count bits |
| Sparse query preparation | O(K+s) | O(K) extra; no s-sized container |
| Accepted branch evaluation | O(K*2^r) | O(K) extra labels and one row table |

Hence the target is **O(s+(r+1)*2^r) arithmetic per accepted query and O(r+1)
EXTRA state, plus paid static O(G) storage**. s<=G for valid sorted unique input.
This is count-word/arithmetic accounting, not Python RSS, a physical RAM cap,
constant-bit arithmetic, or an encoded-size kernel. Large count integers,
assignment masks, `Fraction` normalization, and rational cross-products incur
their own bit costs. A symbolic 10^80 scale is not ingestion of 10^80 features.

The same branch-work units as the existing core are reserved:

```text
N = 2^r
W = K label-node visits + 2*(K row-node visits + E child records + c bundles)
  = 5K-2+2c <= 19r+5
reserved_branch_work = N*W
```

Compute N and N*W first and reject if they exceed the supplied caps, before cloning
records, consuming the sparse stream, or visiting any assignment. Do not enumerate
to discover that a budget is too small. Exact caps are admitted; one unit below
either required cap is refused. A refusal does not validate unread stream entries;
on an admitted query all entries are validated before any orientation. The public
preparation helper does not enumerate orientations at all.

These are logical reservation units, not instructions or latency promises. They
exclude static work, pair validation, indexed reads, record copying, scalar size
checks, rational arithmetic, and Python bookkeeping. Preparation counters expose
`query_core_copies=K` and `query_leaf_lookups=s` on accepted input. `branch_work`
is at most its reservation because infeasible child merges can stop early.

The original compiler's `preprocessing_node_visits=2G-1` counts its contraction
traversal only. The new `ownership_node_visits=2G-1` separately charges the owner
traversal. Neither counter includes all validation, copying, or freezing operations;
all remain linear. For r=1 scaling fixtures, K=4, N=2, and reserved/actual branch
work=48 for every G in 4,8,16,32,64,128,256. Three sparse queries per G use exactly
2, 0, and 1 indexed reads. This is a structural receipt, not a speed ratio.

Building the union/capacity summaries from raw graph data is not implemented or
priced by O(G) supplied-count compilation. Mapping query features to leaf IDs,
deduplicating features, counting intersections, and obtaining sorted pairs are
also caller costs. Sorting an already aggregated unsorted s-entry list can itself
cost O(s log s) comparisons and caller workspace; scanning a dense histogram can
cost O(G). Such upstream costs must be added to any complete-query measurement.
No target selection/access index, graph ingestion, witness expansion, graph query
engine, public workload experiment, practical speedup, or prevalence result is
built here. A07 periodic-counting work remains independently owned.

## Executed Evidence

The [tests](experiments/test_static_core_similarity.py) first ran before the probe
existed: **16 failures**, each `static-core implementation missing`, exit status
one. During pre-implementation fixture review, the all-strict stress metadata was
corrected to satisfy local attained-maxima conditions; the earlier unscaled
fixture would have been invalid. No numerical mismatch was hidden by this change.

After implementation, all 16 tests passed on the first implementation run. Then
the existing core/orientation combination passed **29 tests**, and the full
similarity suite passed **89 tests**, each exit status zero. No wall-clock values
are presented as algorithm or public-workload timing evidence.

| Requirement | Retained verification |
| --- | --- |
| A05S-001 static compile | Exactly one zero-q builder call; owner traversal counters; immutable tuple/table records |
| A05S-002 exact parity | 29,440 small envelopes against actual compatible feature sets AND original orientation/current core; 1,024 deeper eight-leaf original/current-core comparisons |
| A05S-003 sparse preparation | 130 four-leaf prepared-core comparisons covering r=0,1,2,3; all old core fields compared; zero bundles and positive A |
| A05S-004 streamed access | Single-pass iterable and generators; table guard rejects iteration, len, slicing, repeated or unrequested lookups; compiler patched to reject query-time recompilation |
| A05S-005 validation | Invalid pair shape, IDs, duplicates/order, zero/negative/too-large q, sum(q)>a, row sizes, booleans/floats/int subclasses, static dimensions and local capacities |
| A05S-006 bounded refusal | Exact and one-below caps for strict/additive cases; unread streams and zero clone/branch counters on refusal; r=255 reservation factored without enumeration |
| A05S-007 ownership | Mutable inputs released with cyclic GC disabled; nested static mutation refused; repeated queries, interrupted/reentrant streams, and refusals leave static state unchanged |

The 29,440 cases reuse the existing five-feature summary corpus; they are not
29,440 new datasets. Deeper parity is a reference cross-check, not independent
feature enumeration. The 130 prepared-core checks establish equality to the
current builder, not a second independent frontier theorem. The r=255 fixture is
locally admissible metadata for refusal testing; no claim of its global complete
pair feasibility is needed or made. Huge-count checks use scales 1,10^12,10^80,
with and without additional outside-union population. Empty union, a single leaf,
root ties, an impossible companion, and globally impossible metadata are covered.
This work includes source inspection and tests, not an independent code audit.

## Reproduction

Run from the shared repository root; Python 3.11 is the verified interpreter.
`-B` avoids generating bytecode files in existing module directories.

```sh
cd /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover \
  -s research_algorithms_20260920/experiments -p 'test_static_core_similarity.py'
```

Focused and full regression commands:

```sh
cd /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest \
  test_static_core_similarity test_additive_core_similarity test_orientation_parameter_similarity
cd /Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover \
  -s research_algorithms_20260920/experiments -p 'test_*similarity.py'
```

Minimal reusable-core example, from the experiments directory:

```sh
/Users/amuldotexe/.local/bin/python3.11 -B - <<'PY'
from probe_static_core_similarity import compile_static_capacity_core, compute_static_core_bound
core = compile_static_capacity_core([0, 4, 3, 3], [4, 4])
for a in (4, 7):
    stats = {}
    query = ((leaf, 2) for leaf in (0, 1))
    print(compute_static_core_bound(core, a, 4, query,
                                   branch_cap=2, work_cap=34, stats=stats),
          stats["query_leaf_lookups"], stats["query_core_copies"])
PY
```

Expected lines: `3/5 2 3` and `3/8 2 3`. This example has a strict root and K=3,
unlike the additive-root scaling fixture's K=4.

## SHA-256 Receipts

Hashes identify exact implementation/test inputs, not a claim about other agents'
concurrent files. The preexisting core, its tests, orientation baseline/tests,
validator, source note, and review hashes were captured before implementation and
checked again after verification. None of these files was edited by this task.

| Artifact (relative to this note) | SHA-256 |
| --- | --- |
| `experiments/probe_static_core_similarity.py` | `480444ef2add1fd90ccb7af64b3b8e2efe70cf1380e00b1d5af0f4f122602fe2` |
| `experiments/test_static_core_similarity.py` | `a2be37342c1130a3246290198860d2cd0a7fd588ba7d57885aef2f6f3a9d8da3` |
| `experiments/probe_additive_core_similarity.py` | `3088145b00effe0f714d172c51c5d4176be7bde2d7ee7f4c4703efbf8cb669a5` |
| `experiments/test_additive_core_similarity.py` | `b3af0b22ffdd4cd7f304913824007e06e0db8a403af6db714627934d8ca0375b` |
| `experiments/probe_orientation_parameter_similarity.py` | `5f8370f506306918193e0e019cc22b84488b5d2bd81b767b571ab02c687c4779` |
| `experiments/test_orientation_parameter_similarity.py` | `328ce6fc5abfc808931e0384fff89720d5187ed69488d1f2e149aaf65d68d5b4` |
| `experiments/probe_overlap_frontier_similarity.py` | `db4adc5334865f7758d808ebe56682188ba0e54f93a83d32eae40fee4c4cacee` |
| `Similarity-Additive-Region-Core.md` | `020093bd35e6f2acee8b4478abd6d95ff2a6dab4bb4fe02096ba2c4bd1006bf2` |
| `Similarity-Additive-Core-Review.md` | `35b1da303b11b5d647be32d335f07dc24cfa65e5f3bbfecc51df85f804564df5` |

Recompute from the repository root (the C locale avoids the host's Perl locale
failure encountered on the first hash-command attempt):

```sh
env LC_ALL=C LANG=C shasum -a 256 \
  research_algorithms_20260920/experiments/probe_static_core_similarity.py \
  research_algorithms_20260920/experiments/test_static_core_similarity.py \
  research_algorithms_20260920/experiments/probe_additive_core_similarity.py \
  research_algorithms_20260920/experiments/test_additive_core_similarity.py \
  research_algorithms_20260920/experiments/probe_orientation_parameter_similarity.py \
  research_algorithms_20260920/experiments/test_orientation_parameter_similarity.py \
  research_algorithms_20260920/experiments/probe_overlap_frontier_similarity.py \
  research_algorithms_20260920/Similarity-Additive-Region-Core.md \
  research_algorithms_20260920/Similarity-Additive-Core-Review.md
```

The note does not embed its own self-referential hash. No portfolio, journal,
production module, frozen module, commit, or remote branch was modified.

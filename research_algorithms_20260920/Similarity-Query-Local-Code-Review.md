# Query-Local Similarity Code And Accounting Review

Date: 2026-09-21. Bounded independent review. Only this file is owned.

## Findings

### P2: Plan Peak Is Not A Live-Record High-Water Mark

At [probe_query_local_similarity.py:244](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_query_local_similarity.py:244),
the previous iteration's `plan` remains referenced while the right-hand side
builds the next plan. Line 246 records only the largest individual plan.
The retained witness observes **six distinct simultaneously live plan-node
dictionaries while `plan_records_peak` reports three**. This does not change
answers or break the threshold-cell cap, but it invalidates interpreting
that metric as peak live plan records. Release the previous plan before
building the next, isolate a block's evaluation in a helper, or rename the
counter to explicitly mean largest single plan. Do not add independent
category peaks and call the result a measured simultaneous peak.

### P3: Inherited Illustrative Pair Has The Wrong Leaf Maximum

[Similarity-Query-Induced-Review.md:177](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Similarity-Query-Induced-Review.md:177)
gives `{a,b,c,d}` and `{b,e}` for leaves `{b,c}` and `{d,e}`, each supposedly
of capacity two. The second leaf actually has attained maximum one. Replacing
the second row with `{d,e}` realizes the claimed metadata. Numerically the
checker uses rows `{0,2,6,3}` and `{3,7}`, groups four, yielding capacities
`[0,4,1,3,1,0,2,2]`, populations `[1,0,2,2]`, and root `H=5,E=1,C=4`.
This is an example/witness error, **not a rejection of the accepted theorem
or a defect in the new direct-metadata test**. The corrected realization has
exact score `1/4` and old interval `1/2` for query `{0}` and smaller size two.
The inherited file was not edited.

## Correctness Assessment

No score, pruning-safety, or top-k counterexample was found on admitted
compiler-produced inputs. This is a bounded code audit, not universal
certification. The accepted mathematical premises are those in
`Similarity-Query-Induced-Review.md`, especially complete-pair lifting and
the inherited contiguous intersection domain `[E,2C-H]`.

1. **Actual population and E survive.**
   [Plan construction:26](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_query_local_similarity.py:26)
   uses the index's true population and subtracts active children's true
   populations. For an offset base, `floor=lower+upper-actual=E0`, not zero;
   `baseline=lower-debt=b`. Neutral capsules use the same construction.
   The base inverse thresholds are exactly `E0` and `E0+max(0,c-b)`.
   Root checks occur before zero-query evaluation. Inferring `H` from the
   bundle endpoints would be wrong, but this code does not do that.
2. **The inverse recurrence is reused, not newly derived.**
   [Local evaluator:129](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_query_local_similarity.py:129)
   matches the additive convolution and both strict-role branches in
   [inherited evaluator:83](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_compressed_dual_similarity.py:83).
   Strict limits still use `2*C-H-deficit`. A missing fixed-side reward or
   unattainable varying-side threshold discards that branch; the final root
   lookup tests both role tables. Changed inputs are local topology, actual
   offset population, shifted base thresholds, and stronger root admission.
3. **The neutral interval is safe and no larger than the old interval.**
   [Control:67](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_query_local_similarity.py:67)
   starts from exact offset one-row domains/reward functions. Their additive
   convolution has unit slopes followed by saturation, exactly the inherited
   `combine_control_record_group` formula. A strict-node clamp imposes necessary
   row bounds `H-C <= x <= C` but omits attained-maximum coupling, so it is a
   relaxation. For inactive regions the new exact neutral domain is contained
   in the old zero-reward relaxed domain. Domain restriction and convolution
   are monotone; collapsing additive regions preserves this inclusion. Thus
   the local interval is bounded by the old interval at either admitted root
   row size. Both use the true population. This argument depends on valid
   compiled metadata and additive component structure, not arbitrary records.
4. **The `rq=0` shortcut is exact.**
   With no retained strict node the connected reduced tree is one offset or
   neutral root. Its interval reward is exact for each root role, including
   companion feasibility; therefore taking their maximum needs no inverse
   table. The shortcut at
   [line 254](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_query_local_similarity.py:254)
   is valid even with hidden strict nodes and nonzero `E`. Zero threshold
   budgets may still admit this path because it allocates no threshold table.
5. **Top-k ordering and fallback are sound under the builder contract.**
   `(score,-ID)` makes the heap root the worst admitted positive candidate.
   Equal-bound pruning requires every block ID to be strictly worse than that
   root. The builder sorts unique integer IDs; a self ID in `min_id` can make
   pruning weaker but not unsafe. Zero filling only happens with an unfilled
   heap, which means no block could have been pruned while collecting its
   positives. Scanning sorted target IDs then fills zero scores in exact tie
   order, excluding self and positives. Empty queries, outside-union features,
   negative target IDs, duplicate row contents, odd singleton blocks, `k=0`,
   and `k` beyond the eligible population are covered. Profile, skeleton,
   state, and work refusal all retain a safe bound and exact body fallback.

## Budget And Retention Conditions

- **Reservation is correct for the declared recurrence units.**
  [Lines 98-110](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_query_local_similarity.py:98)
  reserve `2*sum(Bv+1)` table cells and the exact counted loop visits, including
  fixed-side lookups and both root scans, before allocating any threshold
  tables. All observed admitted executions use exactly their reservations.
  One-below caps refuse with zero table cells and counted recurrence work.
  The reservation pass itself, plan construction, validation, integer/Fraction
  operations, and allocation/list initialization are not these loop units.
- **Caps are per attempted block, not a whole-query work budget.** The runner
  passes the unchanged `work_cap` on each attempt at
  [line 260](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_query_local_similarity.py:260).
  The witness admits three 44-operation attempts with `work_cap=44` and totals
  132 operations. This is permitted by the present implementation, not a
  reservation arithmetic bug. Unlike the older public driver API, this runner
  has no `query_work_cap`. Public comparisons must disclose this difference
  or enforce the same query-wide policy in the caller. Summed reservation
  metrics also include requests that were refused, so they are not consumed
  work. `neutral` mode and the exact offset shortcut do no threshold work.
- **Skeleton and binary plan sizes differ.** `skeleton_cap` limits marking and
  reduced-record publication, not all transient objects or plan records. If
  `S` reduced records are present, explicit additive merges give a binary
  plan of `P=S+(number of component-child edges) <= 2*S-1`. A threshold refusal
  occurs after preparing the sparse query, skeleton, plan, and interval.
  A nonzero skeleton cap can refuse after partial marking/publication; those
  costs are not zero just because evaluation falls back. The query stream is
  fully materialized as `sparse` before skeleton admission in this runner.
- **Prepared-core costs are not raw-query costs.** At
  [line 210](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_query_local_similarity.py:210),
  `streams` contains one generator per query feature, including absent ones;
  `posting_streams` counts only nonempty postings. The witness has eight
  allocated generators but reports three nonempty streams. Sorting the query,
  hashing, merge setup and posting traversal, sparse list creation, heap
  operations, body intersections, and zero-ID scans need their own accounting.
  `body_memberships += len(features)` is a logical charged row volume, not a
  measurement of Python's actual set-intersection operations.
- **Static retained state is paid separately.** The new index retains three
  `K`-length tuples (`parents`, `populations`, `shared`) plus `R` owner ends,
  and references the static core already retained by the block. The checker
  verifies that `index['static'] is block['core']`; it is not a second copy of
  the core. Defaults, deviations and owner runs remain; full row sets, sizes,
  target IDs, and union-block postings remain. Dense capacities, populations,
  and target postings are not retained by the compact wrapper, but the caller
  must release its original dense source to remove that separate ownership.
  Dense preprocessing is still paid. No physical memory claim follows.
- **No global scan was introduced in the local phase.** Parent marking and
  active adjacency are sparse; inactive totals come from static capacity,
  population, and shared scalars less active children. A strict node examines
  its two child slots. Plan and interval passes visit the local plan only.
  The asymptotic inverse bound remains `O(P*(B+1)+B^2)` counted work and
  `O(P*(B+1))` threshold cells, in addition to preparation. Small support does
  not imply small numeric `B`. This is inherited DP on reduced inputs, not
  a new inverse algorithm or an established practical speedup.

## Coverage And Remaining Gaps

The new test file was read, not executed as a suite. Its existing tests cover
complete small compatible envelopes, full-core parity, positive-E population,
locality guards, budget boundaries, top-k and fallback. Full-core parity
cannot independently certify the recurrence it shares; the retained checker
instead enumerates actual full-union feature pairs for four fixed summaries.
It does not repeat the large exhaustive test matrix or public workloads.

Plans/indexes are trusted compiler-produced records; the solver validator is
not a general hostile-plan parser. Resident sources must preserve sorted
unique IDs, correct row sizes, valid postings, and immutable matching indexes.
The finite oracle uses universes of sizes 0, 5, 4, and 6 with one admitted
root-size pair each; it does not exhaust all trees or root sizes. It tests all
query subsets in those universes, with and without one outside feature. The
seeded top-k cases use eight seven-row sources and a fixed seed. The two modes
and repeated queries are repeated checks, not independent datasets.

No universal complexity improvement over a similarly equipped generic tree
DP, performance prevalence, timing/RSS result, public workload correctness,
or historical-priority claim is established here. Findings above are left for
the lead; no prototype, test, inherited review, or driver was modified.

## Retained Independent Checker

Run from the repository root with Python 3.11; `-B` prevents bytecode writes.
Only standard-library facilities and the audited prototypes are imported.
Lead preservation note: the replay below now selects the archived pre-fix
module whose hash is listed at the end, so the retained lifetime-failure
witness remains reproducible after the live runner is fixed. Its dependencies
remain the unchanged modules in `experiments`. Post-review fixes and tests
are recorded in the execution manuscript, not retroactively credited here.
The oracle constructs actual feature sets and enumerates full-union ordered
pairs; it never calls the prototype's feasibility, winner, or inverse
recurrences. The seeded top-k oracle sorts exact set Jaccard fractions.
No public dataset, timing/RSS measurement, or repository suite is run.

```sh
awk '/^```python$/{emit=1;next} emit && /^```$/{exit} emit{print}' research_algorithms_20260920/Similarity-Query-Local-Code-Review.md | python3.11 -B -
```

```python
from collections import Counter
from fractions import Fraction
from itertools import product
from random import Random
import sys

sys.path.insert(0, "research_algorithms_20260920/experiments")
sys.path.insert(0, "research_algorithms_20260920/evidence/similarity-query-local-20260921")
import probe_query_local_similarity as local
from probe_compressed_interval_similarity import compute_compressed_interval_bounds
from probe_exception_core_similarity import compile_modal_capacity_core
from probe_query_skeleton_similarity import (
    compile_query_skeleton_index, prepare_query_skeleton_core,
)
from profile_large_group_similarity import prepare_large_group_source

COUNTS = Counter()


def compute_independent_tree_counts(row, groups):
    # Direct subtree membership counts, not a tree-DP recurrence.
    counts = [0] * (2 * groups)
    for feature in row:
        node = groups + feature % groups
        while node:
            counts[node] += 1
            node //= 2
    return tuple(counts)


def compute_independent_pair_metadata(rows, groups):
    union = frozenset().union(*rows)
    counts = [compute_independent_tree_counts(row, groups) for row in rows]
    caps = tuple(max(values) for values in zip(*counts))
    populations = compute_independent_tree_counts(union, groups)[groups:]
    return union, min(map(len, rows)), caps, populations


def compute_independent_set_score(query, row):
    overlap = len(query & row)
    return Fraction(overlap, len(query | row)) if overlap else Fraction()


def construct_local_fixture_plan(rows, groups, query):
    union, minimum, caps, populations = compute_independent_pair_metadata(rows, groups)
    static = compile_modal_capacity_core(caps, populations, query_limit=16)
    index = compile_query_skeleton_index(static)
    counts = compute_independent_tree_counts(query & union, groups)[groups:]
    sparse = [(j, q) for j, q in enumerate(counts) if q]
    core = prepare_query_skeleton_core(index, len(query), minimum, sparse)
    plan = local.build_skeleton_threshold_plan(index, core)
    return minimum, static, index, sparse, core, plan


def enumerate_independent_compatible_pairs(rows, groups):
    union, minimum, caps, _ = compute_independent_pair_metadata(rows, groups)
    possible = []
    for labels in product((1, 2, 3), repeat=len(union)):
        first = frozenset(f for f, tag in zip(sorted(union), labels) if tag & 1)
        second = frozenset(f for f, tag in zip(sorted(union), labels) if tag & 2)
        COUNTS["candidate_full_union_pairs"] += 1
        _, actual_min, actual_caps, _ = compute_independent_pair_metadata((first, second), groups)
        if actual_min == minimum and actual_caps == caps:
            possible.append((first, second))
    assert possible
    COUNTS["compatible_full_union_pairs"] += len(possible)
    return union, possible


def check_finite_envelope_fixtures():
    fixtures = (
        (1, (set(), set())),
        (4, ({0, 2, 6, 3}, {3, 7})),
        (2, ({0, 2, 1}, {0, 1, 3})),
        (4, ({0, 4, 2, 3}, {1, 5, 2, 3})),
    )
    for groups, rows in fixtures:
        union, possible = enumerate_independent_compatible_pairs(rows, groups)
        features = sorted(union)
        for mask, outside in product(range(1 << len(features)), (False, True)):
            query = {f for j, f in enumerate(features) if mask & (1 << j)}
            if outside:
                query.add(101)
            minimum, static, _, sparse, core, plan = construct_local_fixture_plan(rows, groups, query)
            expected = max(compute_independent_set_score(query, row)
                           for pair in possible for row in pair)
            paid = {}
            actual = local.evaluate_skeleton_threshold_plan(plan, len(query), minimum, stats=paid)
            control = local.evaluate_skeleton_interval_control(plan, len(query), minimum)
            old = compute_compressed_interval_bounds(static, len(query), minimum, sparse)["interval"]
            assert actual == expected, (rows, query, actual, expected)
            assert actual <= control <= old, (rows, query, actual, control, old)
            assert paid["states"] == paid["reserved_states"]
            assert paid["operations"] == paid["reserved_operations"]
            COUNTS["envelope_comparisons"] += 1
            COUNTS["interval_strict_improvements"] += control < old
            COUNTS["positive_floor_plans"] += any((n["floor"] or 0) > 0 for n in plan["nodes"])
            if core["strict_nodes"] == 0:
                assert control == actual
                COUNTS["zero_strict_exact_checks"] += 1
        COUNTS["envelope_fixtures"] += 1
    rows, query = ({0, 2, 6, 3}, {3, 7}), {0}
    minimum, _, _, _, _, plan = construct_local_fixture_plan(rows, 4, query)
    root = plan["nodes"][plan["root"]]
    assert (root["population"], root["floor"], root["capacity"]) == (5, 1, 4)
    assert local.evaluate_skeleton_threshold_plan(plan, 1, minimum) == Fraction(1, 4)
    print("offset_witness H=5 E=1 C=4 exact=1/4 old_interval=1/2")


def compute_independent_topk_rows(targets, query, sid, k):
    eligible = [(node, frozenset(row)) for node, row in targets if node != sid]
    eligible.sort(key=lambda pair: (-compute_independent_set_score(query, pair[1]), pair[0]))
    return [(node, len(query), len(row), len(query & row)) for node, row in eligible[:k]]


def check_seeded_topk_queries():
    rng = Random(9217401)
    ids = (-7, -2, 0, 3, 8, 13, 21)
    for trial in range(8):
        groups = (1, 2, 4, 8)[trial % 4]
        targets = [(node, {f for f in range(10) if rng.randrange(4) == 0}) for node in ids]
        targets[1] = (ids[1], targets[0][1].copy())
        targets[-1] = (ids[-1], set())
        rng.shuffle(targets)
        source = local.prepare_skeleton_source_view(prepare_large_group_source(targets, groups, query_limit=16))
        queries = (set(), {101}, {0, 1, 2}, {f for f in range(12) if rng.randrange(2)})
        for query, (sid, k), mode in product(queries, ((None, 0), (None, 3), (-7, 20), (999, 1)), ("neutral", "dual")):
            actual, stats = local.run_skeleton_similarity_query(source, query, sid, k, mode=mode)
            assert actual == compute_independent_topk_rows(targets, query, sid, k)
            assert stats["dense_histogram_cells"] == 0
            COUNTS["seeded_topk_comparisons"] += 1
        COUNTS["seeded_target_sets"] += 1
    targets = [(9, {1}), (-4, {1}), (2, {1}), (11, set()), (15, {2})]
    source = local.prepare_skeleton_source_view(prepare_large_group_source(targets, 4))
    for query, sid, k in (({1}, None, 2), ({1}, -4, 2), (set(), None, 4), ({99}, 2, 9)):
        for mode in ("neutral", "dual"):
            actual, _ = local.run_skeleton_similarity_query(source, query, sid, k, mode=mode)
            assert actual == compute_independent_topk_rows(targets, query, sid, k)
            COUNTS["tie_zero_self_comparisons"] += 1


def check_exact_budget_boundaries():
    fixtures = (
        (4, ({0, 2, 6, 3}, {3, 7}), {0}),
        (2, ({0, 2, 1}, {0, 1, 3}), {0, 1, 2}),
        (4, ({0, 4, 2, 3}, {1, 5, 2, 3}), {0, 1, 2}),
    )
    for groups, rows, query in fixtures:
        minimum, _, index, sparse, core, plan = construct_local_fixture_plan(rows, groups, query)
        paid = {}
        expected = local.evaluate_skeleton_threshold_plan(plan, len(query), minimum, stats=paid)
        states, work = paid["states"], paid["operations"]
        for state_cap, work_cap in ((states - 1, work), (states, work - 1), (states, work)):
            costs = {}
            try:
                answer = local.evaluate_skeleton_threshold_plan(plan, len(query), minimum,
                    state_cap=state_cap, work_cap=work_cap, stats=costs)
            except local.QueryThresholdBudgetExceeded:
                assert state_cap < states or work_cap < work
                assert costs["states"] == costs["operations"] == 0
                COUNTS["threshold_boundary_refusals"] += 1
            else:
                assert (state_cap, work_cap) == (states, work)
                assert answer == expected
                COUNTS["threshold_boundary_admissions"] += 1
            assert (costs["reserved_states"], costs["reserved_operations"]) == (states, work)
        size = len(core["nodes"])
        for cap in (size - 1, size):
            try:
                admitted = prepare_query_skeleton_core(index, len(query), minimum, sparse, skeleton_cap=cap)
            except ValueError as error:
                assert cap == size - 1 and str(error).startswith("skeleton budget")
                COUNTS["skeleton_boundary_refusals"] += 1
            else:
                assert cap == size and admitted == core
                COUNTS["skeleton_boundary_admissions"] += 1
        print("boundary", "plan_nodes=" + str(len(plan["nodes"])), "states=" + str(states), "work=" + str(work))
    rows = ({0, 2, 6, 3}, {3, 7})
    minimum, _, index, _, _, plan = construct_local_fixture_plan(rows, 4, set())
    assert local.evaluate_skeleton_threshold_plan(plan, 0, minimum) == 0
    for invalid in (1, 5):
        try:
            local.evaluate_skeleton_threshold_plan(plan, 0, invalid)
        except ValueError:
            COUNTS["empty_query_infeasible_rejections"] += 1
        else:
            raise AssertionError("empty query accepted infeasible root")
    targets = [(0, {0, 20, 21, 22, 23}), (1, {80}),
               (2, {0, 2, 6, 3}), (3, {3, 7})]
    source = local.prepare_skeleton_source_view(prepare_large_group_source(targets, 4))
    actual, costs = local.run_skeleton_similarity_query(source, {0}, None, 1, state_cap=0, work_cap=0)
    assert actual == compute_independent_topk_rows(targets, {0}, None, 1)
    assert costs["exact_offset_shortcuts"] == 1 and costs["threshold_attempts"] == 0
    COUNTS["zero_budget_offset_shortcuts"] += 1


def check_fallback_and_accounting():
    targets = [(0, {0, 1, 2, 77}), (1, {4, 5})]
    targets += [(node, {0, 1, 3} if node % 2 == 0 else {0, 2, 3}) for node in range(2, 8)]
    query = {0, 1, 2}
    for limit, options, metric in (
        (0, {}, "profile_refusals"),
        (10, {"skeleton_cap": 0}, "skeleton_refusals"),
        (10, {"state_cap": 0}, "threshold_refusals"),
        (10, {"work_cap": 0}, "threshold_refusals"),
    ):
        source = local.prepare_skeleton_source_view(prepare_large_group_source(targets, 4, query_limit=limit))
        actual, stats = local.run_skeleton_similarity_query(source, query, None, 1, **options)
        assert actual == compute_independent_topk_rows(targets, query, None, 1)
        assert stats[metric] > 0
        COUNTS["fallback_path_comparisons"] += 1
    source = local.prepare_skeleton_source_view(prepare_large_group_source(targets, 4, query_limit=10))
    assert "target_posts" not in source
    for block in source["blocks"]:
        assert "caps" not in block and "populations" not in block
        assert block["index"]["static"] is block["core"]
    # Observe the runner's still-live previous plan while its new RHS is built.
    # This does not retain old plans outside the call or alter prototype files.
    original = local.build_skeleton_threshold_plan
    observed = Counter()

    def inspect_overlapping_plan_lifetimes(index, core):
        caller = sys._getframe(1).f_locals
        old = caller.get("plan")
        new = original(index, core)
        previous = 0 if old is None else len(old["nodes"])
        if old is not None:
            assert not ({id(n) for n in old["nodes"]} & {id(n) for n in new["nodes"]})
        observed["live_plan_records"] = max(observed["live_plan_records"], previous + len(new["nodes"]))
        observed["allocated_streams"] = len(caller["streams"])
        observed["nonempty_streams"] = caller["stats"]["posting_streams"]
        return new

    local.build_skeleton_threshold_plan = inspect_overlapping_plan_lifetimes
    try:
        extended = query | {100, 101, 102, 103, 104}
        actual, stats = local.run_skeleton_similarity_query(source, extended, None, 1)
    finally:
        local.build_skeleton_threshold_plan = original
    assert actual == compute_independent_topk_rows(targets, extended, None, 1)
    assert observed["live_plan_records"] > stats["plan_records_peak"]
    assert observed["allocated_streams"] > observed["nonempty_streams"]
    print("accounting", "live_plan_records=" + str(observed["live_plan_records"]),
          "reported_plan_peak=" + str(stats["plan_records_peak"]),
          "allocated_streams=" + str(observed["allocated_streams"]),
          "reported_nonempty_streams=" + str(observed["nonempty_streams"]))
    attempts = stats["threshold_evaluations"]
    assert attempts == 3 and stats["operations"] % attempts == 0
    per_attempt = stats["operations"] // attempts
    actual, costs = local.run_skeleton_similarity_query(source, extended, None, 1, work_cap=per_attempt)
    assert actual == compute_independent_topk_rows(targets, extended, None, 1)
    assert costs["operations"] == per_attempt * attempts > per_attempt
    assert costs["threshold_refusals"] == 0
    print("budget_scope", "per_attempt_cap=" + str(per_attempt),
          "query_operations=" + str(costs["operations"]), "accepted_attempts=" + str(attempts))
    COUNTS["accounting_witnesses"] += 3


check_finite_envelope_fixtures()
check_seeded_topk_queries()
check_exact_budget_boundaries()
check_fallback_and_accounting()
for name in sorted(COUNTS):
    print(name + "=" + str(COUNTS[name]))
print("PASS: bounded code audit; no public workloads or suites")
```

## Executed Results

The retained command completed with exit code zero under Python 3.11.
An initial checker run reproduced the inherited illustrative-pair mistake
and stopped at the dedicated `(H,E,C)` assertion. Only the review's fixture
was corrected to the actual realization described above; no prototype
failure was suppressed. These are the exact counts from the final fresh run,
not cumulative counts across development runs:

```text
offset_witness H=5 E=1 C=4 exact=1/4 old_interval=1/2
boundary plan_nodes=1 states=4 work=8
boundary plan_nodes=3 states=18 work=44
boundary plan_nodes=5 states=26 work=52
accounting live_plan_records=6 reported_plan_peak=3 allocated_streams=8 reported_nonempty_streams=3
budget_scope per_attempt_cap=44 query_operations=132 accepted_attempts=3
accounting_witnesses=3
candidate_full_union_pairs=1054
compatible_full_union_pairs=19
empty_query_infeasible_rejections=2
envelope_comparisons=226
envelope_fixtures=4
fallback_path_comparisons=4
interval_strict_improvements=2
positive_floor_plans=6
seeded_target_sets=8
seeded_topk_comparisons=256
skeleton_boundary_admissions=3
skeleton_boundary_refusals=3
threshold_boundary_admissions=3
threshold_boundary_refusals=6
tie_zero_self_comparisons=8
zero_budget_offset_shortcuts=1
zero_strict_exact_checks=16
PASS: bounded code audit; no public workloads or suites
```

The 1,054 candidate ordered pairs yield 19 compatible ordered pairs across
four fixed metadata fixtures. All 226 query/envelope cases independently
check exactness and `exact <= neutral <= old`; two have a strictly tighter
neutral interval. Six plans have a positive base floor; 16 have no retained
strict node and verify interval exactness. Three representative plans cover
offset-only, strict, and additive-plus-strict budget accounting. Six
one-below threshold refusals and three exact admissions were observed, plus
three skeleton boundary refusals/admissions. The direct zero-budget resident
shortcut is separate from those three threshold admissions.

The accounting probe temporarily wraps a module function **in memory** to
observe the runner's existing previous-plan reference while the new RHS
returns; it restores the function in `finally`. It neither changes files nor
artificially stores old plans between calls. Six counts distinct node
dictionaries, not two references to the same three nodes. These observations
are logical object counts, not allocation-byte or RSS measurements.

## Reviewed Snapshot

SHA-256 of the reviewed code, relative to `research_algorithms_20260920/experiments/`:

```text
a03c2fe681db2f0dcc496df1213bd56ec9d1d497fcfdd2474e442fc467ca7b8d  probe_query_local_similarity.py
0fc3d20c72a24d3ba004cb9b1869eda3417e3453b5f30c9e711e72cd1f90e631  test_query_local_similarity.py
38cb3ea1021c2b0821d4e731e69a9c1384be2698dc13820cc1650ac8708b73b4  probe_query_skeleton_similarity.py
a3b2e2af5be94f4d120090e9eaf29a35ebe021396131cd061a13afa8f2df1bbf  probe_compressed_dual_similarity.py
28932a0ea9395fdcf6d86e27426fc08662d4db40fb84596faf0fd4641a5039d0  probe_compressed_interval_similarity.py
```

Bounded review complete. Only `Similarity-Query-Local-Code-Review.md` was
written. No commit, public driver run, dataset access, full suite, or timing/RSS
measurement was performed.

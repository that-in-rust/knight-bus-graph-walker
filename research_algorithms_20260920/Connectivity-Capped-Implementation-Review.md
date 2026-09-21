# A02 Capped Connectivity Implementation Review

Date: 2026-09-21. Independent, read-only implementation audit except for this
new document. No SQLite-internal review, performance benchmark, code change,
other-document edit, or commit. This is a logical-work/algorithm review,
not a physical memory cap or a global novelty claim.

## Findings

**No actionable scheduler correctness or asymptotic-workspace defect found
under the trusted, immutable prepared-source contract.** This conclusion is
supported by independent execution and a source-level invariant audit below,
not just the supplied green tests. It includes the final
`core_plan="complement"` addition: `cap_enabled=False` disables the budget
sentinel only (line 47); the uncapped path leaves every deferred bit zero.

**Accounting caveat for integration, not a code bug:** the theorem's `h_f`
belongs to the CAPPED execution's frozen factor-entry state. An independently
run complement control may complete unions sooner, causing later factors to
skip or have a larger seed class. Its per-factor costs are not interchangeable
with those counterfactual `h_f` values. The checker recomputes each uncapped
reference schedule at the actual capped entry, before local unions occur.

Concrete witness: factors `{0,1}`, `{0,2}`, `{0,1}`, `{0,3,4,5,6,7}`;
cover `{0,1,2}`; delete `03,04,05,06,07`. The capped run spends `4+4+4=12`
events and 12 fallback postings. At its entries each of the first three
factors has `q=4,h=7`. The separate complement run spends `7+7+0=14`:
its third factor is already resolved. Using that run to infer
`K=min(4,7)+min(4,7)+0=8` would be incorrect. Likewise `48+48` below means
local events plus fallback posting records, not total workflow cost.

## Evidence Receipt

Successful independent executions: **144,418**.

| Family | Executions | Coverage |
| --- | ---: | --- |
| Exhaustive, 3 original vertices | 2,880 | Every ordered two-factor incidence pattern, every valid cover, every permitted edit subset. |
| Exhaustive, 4 original vertices | 139,520 | Same enumeration; includes duplicate/empty factors and overlapping deletions. |
| Seeded randomized | 2,000 | 1-9 vertices, 0-8 factors, noncontiguous IDs, unsorted vertex/posting/deletion order, arbitrary valid nonminimal covers. |
| Named tiny capped cases | 8 | Matching, duplicate-factor replay, retained partial unions, dense negatives, floor seeding, outside insertion minimum, overlap isolation, empty universe. |
| Same named cases, complement | 8 | Exact full uncapped event sequences, zero caps/fallback. |
| Replay adversary, both plans | 2 | Capped: 48 events + 48 fallback postings, one fallback. Complement: 1,224 events, no fallback. |

The first 144,408 executions exercised 14,022 interrupted factor searches:
390 stops before an open, 11,645 before a record, and 1,987 before a candidate
test. Thus the check reaches partial deletion rows as well as exact row and
test boundaries. Across the final corpus, 11,763 executions used fallback.
In 1,700 executions, completed local unions were checked still connected at
fallback entry; the named retained-union case specifically caps the SAME
factor after a successful union, before a later candidate test.

Each successful execution checks all original IDs and exact minimum labels
against expanded transitive closure; every helper union connects vertices
in a true final component. The checker also verifies actual cursor open/read
ORDER against the independently predicted event prefix, each category's
count, the exact cap/deferred budget, one-or-zero fallback passes, exactly
one fallback deletion/membership sweep, the posting bound, per-row directed
candidate deduplication, and all acquired cursors closed before the next open.

**Failure injection:** 417 expected failures across seven call surfaces:
local posting open, deletion cursor open/read/close, fallback deletion row,
fallback membership row, and fallback posting. Each replay substitutes an
`OSError` subclass, `ValueError`, or `StopIteration` at a specific observed
call. All propagated with no labels emitted; local failures did not enter
fallback, and acquired cursors had one close attempt. Another three cases
rejected changed, negative, and boolean row lengths and closed their cursors.
The fake close marks its cursor closed before raising: this verifies executor
cleanup/control flow, NOT successful resource release by a failing provider.
The injection does not simulate silent early EOF of the complete iterator
APIs, corrupt same-length content, or every provider's internal streaming I/O.

Existing suite replay commands, with bytecode writes disabled:

```sh
cd research_algorithms_20260920/experiments
env PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest -v \
  test_capped_fault_connectivity test_fault_cover_connectivity
```

The initial snapshot passed 8 capped + 9 legacy tests. The final snapshot
adds the complement comparator test; its fresh replay passed **18 tests
(9 capped/comparator + 9 legacy), OK**. The complete independent checker was
also replayed successfully after that addition, with the counts above.
Source, theorem, and test hashes were rechecked after both final runs and
matched the table below. All correctness execution is finished; the lead
was explicitly notified before its serial performance run. No performance
benchmark was run in this review.

## Theorem And Source Audit

All line numbers refer to `experiments/probe_fault_cover_connectivity.py`.

| Obligation | Source evidence and reasoning |
| --- | --- |
| Complete quotient before search | Lines 253-333 finish outside contraction, exact uncertain attachments, outside insertions, and core-core insertions before the helper call at 335-340. No pending outside connection is assumed by a seed. |
| Overlap cannot revive a deletion | Rows are GLOBAL deletions at 55-69 and 121-126, not factor-local absences. Every candidate is filtered against the relevant complete row. Duplicate factor IDs remain separately scheduled but cannot authorize a deleted edge. |
| Correct resolved gate | Lines 29-32 skip at most one core or a single existing core class. Any surviving core-outside edge was attached already; if no core is attached, all such edges are absent. The gate does not fabricate an outside connector. |
| Frozen outside seed class | Lines 33-39 take `R_f` only from the factor root when outside count is positive. `blocked` is its frozen complement, `q=t*p`, and core-only factors get no seed root. Core deletion degree (282-284) upper-bounds deleted seed neighbors, so `c_s < |R_f|` soundly certifies a surviving seed edge. |
| Safe seed counts and partial marks | Seed unions at 88-91 follow either the degree certificate or a COMPLETE counted row. BFS union tests at 99-106 follow a COMPLETE marking row. A cap during either scan exits through `finally`; no partial count or marks reach a union. |
| Ordinary complement traversal | Lines 74-107 remove/enqueue each blocked position once; row epoch is its unique position+1. Linked-list successor is captured before removal. An empty unvisited list stops search without reading queued tails. No changing-root shortcut changes the frozen reference schedule. |
| Exact event cap | Lines 45-62 and 102 charge before the next open, record request, or candidate test. Exact cached length avoids an uncharged EOF fetch. Normal runs consume exactly `min(q,h)`; reaching `q` is not itself a cap unless another metered event is required. |
| Sound retained work | Lines 109-112 retain DSU unions and one deferred bit only. They do not roll back justified unions or retain an incomplete row as a certificate. Named fixture: factors `{0,1,2}`, `{1,3,4,5,6}`, delete `02,13,14,15,16`, cover `{0,1,2}`. The first factor unions `01`, spends 9 events, caps before testing `12`, then shared fallback restores that remaining edge. |
| Single row-major fallback | Lines 114-145 lie OUTSIDE the factor loop. There is one deletion-row pass over cover IDs and one membership pass, independent of how many factors cap. A previously attached endpoint cannot detach, so scanning endpoints are a subset of entry `B_f`, proving at most `t*p` postings per deferred factor. |
| Candidate dedup is row-local | Lines 117-145 use epoch `rank+1`, mark a candidate only after deletion filtering, and collect each other core at most once across overlapping deferred factors. All posting records are still paid. Reverse orientations in different rows are intentional, not a dedup bug. The duplicate replay fixture reads 12 fallback postings but makes only four directed candidate visits. |
| Failure is not a cap | Only `LocalSearchBudgetExhausted` is caught (109). Provider exceptions escape; cleanup is in `finally` (70-71). A close failure during budget cancellation aborts rather than pretending fallback succeeded. Providers must not raise the scheduler's internal sentinel themselves. |
| Full output and minima | Lines 194-198 initialize core minima; 236-250 propagate minima; 253-270 contribute outside and insertion-only endpoints. Output at 342-360 starts only after successful helper/fallback completion and streams all original IDs, including isolates. |

### Workspace Bound

The kernel retains `O(F+k)` WORDS under the prepared-view contract:

* Caller DSU/minima have `F+k` entries; factor counts/stamps have `F`; core
  dictionaries, degree/length caches, stamps, and seen flags have `k`.
* The caller's last `memberships` tuple (line 276) can remain live while the
  helper runs, but has at most `F` entries, not `F*k`.
* Helper `deferred` has `F` bits/bytes. `members`, `seeds`, `blocked`,
  `positions`, links, forbidden marks and queue are for ONE factor, each at
  most `k`. The next factor replaces them; no per-factor list is appended
  into a retained container. The nested functions capture reused cells,
  not a growing set of saved factor frames.
* Last-factor locals can survive into fallback. This is another constant
  multiple of `k`, not zero memory and not `F*k`. Fallback adds two `k`
  stamp arrays and at most `k-1` candidates per row, then replaces the list.
* Per-factor allocation/initialization is `O(t_f)` (blocked arrays are
  `p_f<=t_f`); there is no hidden `k`-clear inside every factor. Source row
  lengths affect iteration count, not retained deletion-row storage.

This is a reachability/allocation audit, not an RSS measurement. Unique
members and immutable, bounded source iterators/cursors remain prerequisites.
The kernel's `max_state_nodes` checks DSU nodes only, as its docstring says.
Provider caches, fixture histories, caller output dictionaries, Python
allocator high-water marks and exception tracebacks are not a proved
physical cap. No claim of superior state to the existing `O(F+k)` DFS
fragment control, or of a novel complement BFS primitive, follows.

### Residual Test Gaps

The shipped capped tests cover one metadata error but not the independent
open/read/close failure matrix. Preserving those regressions in the regular
suite would protect the source integration; this review changes no tests.
Prepared-source normalization, snapshot consistency on EVERY view, silent
stream truncation, sink failures/partial output after emission starts, and
SQLite resource release are outside this scheduler-only review. Failed
queries do not return a successful receipt; pre-call charges on an exception
path should not be read as completed physical I/O.

## Reviewed Snapshot

SHA-256, obtained with `env LC_ALL=C shasum -a 256`:

| File | SHA-256 |
| --- | --- |
| `experiments/probe_fault_cover_connectivity.py` | `3c1b4a85c161149aeacaa467ef5871ea574f65b540c4ca93e792b0c035182bec` |
| `Connectivity-Cover-Complement-Study.md` | `564e2b47c09441897411fde808cf9e93c2d4625a54bb13d8c8453f8db604f04b` |
| `experiments/test_capped_fault_connectivity.py` | `79fc021fc44c1729e65170c855f80db321e28147fb121613b029d12e0db412ea` |
| `experiments/test_fault_cover_connectivity.py` | `5a71e68ccf553ce5f0d47e4e8f8268ec721a17a5252fcd80fd05f6ef83c33edd` |

Paths above are relative to `research_algorithms_20260920/`. All source line
references in this document refer to this hash, not a moving integration checkout.

## Reproducible Independent Checker

The fixture, expanded transitive-closure oracle, uncapped reference schedule,
and failure injector below are independently written here. Only the two
production entry points are executed from the reviewed module; no existing
test fixture, expected-label function, or theorem-checker code is imported.
Reference BFS uses ordered lists/sets, not the executor's indexed linked list.
The harness wraps the helper in memory only; it never changes its source.
Resident oracle data, operation logs, and reference event sequences are TEST
state, not evidence of executor workspace usage.

From the repository root, run just this standard-library correctness checker:

```sh
awk '/^```python$/{p=1;next} p && /^```$/{exit} p' \
  research_algorithms_20260920/Connectivity-Capped-Implementation-Review.md \
  | env PYTHONDONTWRITEBYTECODE=1 python3 -B -
```

```python
from collections import Counter
from itertools import combinations
import random
import sys

sys.path.insert(0, "research_algorithms_20260920/experiments")
import probe_fault_cover_connectivity as lead


class InjectedSourceReadFailure(OSError):
    pass


def build_expanded_label_oracle(vertices, groups, deleted, added):
    vertices = sorted(vertices)
    index = {v: i for i, v in enumerate(vertices)}
    edges = set().union(*(set(combinations(sorted(g), 2)) for g in groups))
    edges = (edges - set(deleted)) | set(added)
    reach = [1 << i for i in range(len(vertices))]
    for u, v in edges:
        a, b = index[u], index[v]
        reach[a] |= 1 << b
        reach[b] |= 1 << a
    for mid in range(len(vertices)):
        for row in range(len(vertices)):
            if reach[row] & (1 << mid):
                reach[row] |= reach[mid]
    return {v: min(w for j, w in enumerate(vertices) if reach[i] & (1 << j))
            for i, v in enumerate(vertices)}


def compute_uncapped_factor_events(source, factor):
    selected, outside, find, receipt = source.context
    total_factors = source.factor_count
    nodes = {v: total_factors + i for i, v in enumerate(selected)}
    members = source.core[factor]
    if len(members) <= 1 or len({find(nodes[v]) for v in members}) == 1:
        return 0, []
    seeds = {v for v in members
             if outside[factor] and find(nodes[v]) == find(factor)}
    blocked = [v for v in members if v not in seeds]
    remaining, queue, events = set(blocked), [], []
    def record_complete_row_events(vertex):
        events.append(("open", vertex))
        events.extend(("read", vertex, w) for w in source.negative[vertex])
        return set(source.negative[vertex])
    if seeds:
        for vertex in blocked:
            degree = sum(w in source.cover for w in source.negative[vertex])
            if degree < len(seeds) or len(record_complete_row_events(vertex) & seeds) < len(seeds):
                remaining.remove(vertex)
                queue.append(vertex)
    while remaining:
        if not queue:
            vertex = next(v for v in blocked if v in remaining)
            remaining.remove(vertex)
            queue.append(vertex)
            if not remaining:
                break
        vertex = queue.pop(0)
        forbidden = record_complete_row_events(vertex)
        for other in blocked:
            if other in remaining:
                events.append(("test", vertex, other))
                if other not in forbidden:
                    remaining.remove(other)
                    queue.append(other)
    return len(members) * len(blocked), events


class IndependentDeletionCursorFixture:
    def __init__(self, source, vertex):
        self.source, self.vertex = source, vertex
        self.row = source.negative[vertex]
        self.length = len(self.row)
        if source.metadata is not None:
            self.length = source.metadata(self.length)
        self.position, self.closed = 0, False

    def read_next_neighbor_exact(self):
        assert not self.closed
        self.source.touch_source_operation("cursor_read")
        assert self.position < len(self.row), "unbudgeted EOF probe"
        neighbor = self.row[self.position]
        self.position += 1
        self.source.actual_io.append(("read", self.vertex, neighbor))
        return neighbor

    def close_current_cursor_exact(self):
        assert not self.closed
        self.closed = True
        self.source.touch_source_operation("cursor_close")


class IndependentPreparedSourceFixture:
    snapshot_id = "independent-review-v1"

    def __init__(self, vertices, groups, deleted=(), added=(), cover=(),
                 fail_at=None, metadata=None, error_type=InjectedSourceReadFailure):
        self.vertices, self.groups = tuple(vertices), tuple(map(tuple, groups))
        self.deleted, self.added, self.cover = tuple(deleted), tuple(added), set(cover)
        self.vertex_count, self.factor_count = len(vertices), len(groups)
        self.rows = {v: tuple(f for f, g in enumerate(groups) if v in g) for v in vertices}
        self.core = [tuple(v for v in g if v in self.cover) for g in groups]
        self.negative = {v: [] for v in vertices}
        self.positive = {v: [] for v in vertices}
        for pairs, rows in ((deleted, self.negative), (added, self.positive)):
            for u, v in pairs:
                rows[u].append(v)
                rows[v].append(u)
        self.labels = build_expanded_label_oracle(vertices, groups, deleted, added)
        self.node_labels = []
        for group in groups:
            outside = [v for v in group if v not in self.cover]
            self.node_labels.append(self.labels[outside[0]] if outside else None)
        self.node_labels += [self.labels[v] for v in sorted(cover)]
        self.fail_at, self.metadata, self.error_type = fail_at, metadata, error_type
        self.context, self.receipt = None, None
        self.operations, self.opened, self.actual_io, self.expected_io = [], [], [], []
        self.expected_counts, self.boundaries = Counter(), Counter()
        self.expected_caps, self.expected_budget = 0, 0
        self.saved_unions, self.fallback_pairs = [], set()
        self.retained_checked = False

    def touch_source_operation(self, operation):
        phase = "outside_helper"
        if self.context is not None:
            phase = "fallback" if self.receipt["fallback_passes"] else "local"
        self.operations.append((phase, operation))
        if len(self.operations) == self.fail_at:
            raise self.error_type("injected " + phase + ":" + operation)

    def iterate_complete_edit_edges(self):
        self.touch_source_operation("edits")
        return iter(self.deleted + self.added)

    def iterate_complete_vertex_rows(self):
        self.touch_source_operation("vertices")
        return ((v, iter(self.rows[v]), iter(self.positive[v])) for v in self.vertices)

    def iterate_inserted_cover_edges(self):
        self.touch_source_operation("insertions")
        return ((u, v) for u, v in self.added if u in self.cover and v in self.cover)

    def iterate_vertex_factor_memberships(self, vertex):
        self.touch_source_operation("memberships")
        return iter(self.rows[vertex])

    def iterate_vertex_deleted_neighbors(self, vertex):
        self.touch_source_operation("deletions")
        if self.context and self.receipt["fallback_passes"] and not self.retained_checked:
            find = self.context[2]
            assert all(find(u) == find(v) for u, v in self.saved_unions)
            self.retained_checked = True
        return iter(self.negative[vertex])

    def iterate_factor_cover_members(self, factor):
        self.touch_source_operation("posting")
        if self.context and not self.receipt["fallback_passes"]:
            budget, events = compute_uncapped_factor_events(self, factor)
            prefix = events[:budget] if self.cap_enabled else events
            self.expected_counts.update(e[0] for e in prefix)
            self.expected_io.extend(e for e in prefix if e[0] != "test")
            if self.cap_enabled and len(events) > budget:
                self.expected_caps += 1
                self.expected_budget += budget
                self.boundaries[events[budget][0]] += 1
        return iter(self.core[factor])

    def open_cover_deletion_row(self, snapshot_id, vertex):
        self.touch_source_operation("cursor_open")
        assert snapshot_id == self.snapshot_id
        assert all(c.closed for c in self.opened), "overlapping cursor lifetimes"
        cursor = IndependentDeletionCursorFixture(self, vertex)
        self.opened.append(cursor)
        self.actual_io.append(("open", vertex))
        return cursor


original_helper = lead.execute_capped_core_search


def wrap_capped_search_checker(source, snapshot, selected, core_index, outside,
                               lengths, degrees, find, merge, receipt, *, cap_enabled=True):
    source.context = selected, outside, find, receipt
    source.cap_enabled = cap_enabled
    source.receipt = receipt
    assert lengths == [len(source.negative[v]) for v in selected]
    assert degrees == [sum(w in source.cover for w in source.negative[v]) for v in selected]
    def verify_sound_union_operation(left, right):
        assert source.node_labels[left] is not None
        assert source.node_labels[left] == source.node_labels[right], "unsound local union"
        if receipt["fallback_passes"]:
            pair = left, right
            assert pair not in source.fallback_pairs, "duplicate directed fallback candidate"
            source.fallback_pairs.add(pair)
        else:
            source.saved_unions.append((left, right))
        return merge(left, right)
    try:
        original_helper(source, snapshot, selected, core_index, outside, lengths,
                        degrees, find, verify_sound_union_operation, receipt, cap_enabled=cap_enabled)
        assert source.actual_io == source.expected_io
        for event, key in (("open", "local_deletion_opens"), ("read", "local_deletion_records"),
                           ("test", "local_candidate_tests")):
            assert receipt[key] == source.expected_counts[event], (key, receipt, source.expected_counts)
        assert receipt["local_search_events"] == sum(source.expected_counts.values())
        assert receipt["core_search_caps"] == source.expected_caps
        assert receipt["deferred_scan_budget"] == source.expected_budget
        assert receipt["fallback_core_posting_reads"] <= source.expected_budget
        assert source.expected_budget <= receipt["local_search_events"]
        assert receipt["fallback_passes"] == bool(source.expected_caps)
        assert receipt["fallback_negative_opens"] == len(selected) * bool(source.expected_caps)
        assert receipt["fallback_negative_records"] == sum(lengths) * bool(source.expected_caps)
        assert receipt["fallback_membership_reads"] == sum(len(source.rows[v]) for v in selected) * bool(source.expected_caps)
    finally:
        source.context = None


lead.execute_capped_core_search = wrap_capped_search_checker
totals, boundaries = Counter(), Counter()


def run_independent_fixture_check(case, category, plan="capped"):
    source = IndependentPreparedSourceFixture(*case)
    output = []
    receipt = lead.run_prepared_fault_connectivity(
        source, case[4], lambda *row: output.append(row),
        max_state_nodes=source.factor_count + len(case[4]), core_plan=plan)
    assert len(output) == len(case[0]) and len(dict(output)) == len(case[0])
    assert dict(output) == source.labels, (case, output, source.labels)
    assert all(c.closed for c in source.opened)
    assert receipt["local_deletion_opens"] == len(source.opened)
    assert receipt["local_deletion_records"] == sum(c.position for c in source.opened)
    totals[category] += 1
    totals["capped_executions"] += bool(source.expected_caps)
    totals["retained_union_executions"] += bool(source.saved_unions and source.retained_checked)
    boundaries.update(source.boundaries)
    return source, output


for size in (3, 4):
    vertices = tuple(range(size))
    pairs = tuple(combinations(vertices, 2))
    for incidence in range(1 << (2 * size)):
        groups = tuple(tuple(v for v in vertices if incidence & (1 << (f * size + v))) for f in range(2))
        base = set().union(*(set(combinations(g, 2)) for g in groups))
        for cover_bits in range(1 << size):
            cover = tuple(v for v in vertices if cover_bits & (1 << v))
            editable = tuple(e for e in pairs if any(v in cover for v in e))
            for toggle_bits in range(1 << len(editable)):
                edits = {e for i, e in enumerate(editable) if toggle_bits & (1 << i)}
                case = vertices, groups, tuple(sorted(edits & base)), tuple(sorted(edits - base)), cover
                run_independent_fixture_check(case, "exhaustive_n" + str(size))

rng = random.Random(2026092102)
for _ in range(2000):
    vertices = tuple(rng.sample(range(1000), rng.randrange(1, 10)))
    groups = tuple(tuple(v for v in vertices if rng.randrange(2)) for _ in range(rng.randrange(9)))
    cover = tuple(v for v in vertices if rng.randrange(2))
    base = set().union(*(set(combinations(sorted(g), 2)) for g in groups))
    edits = {e for e in combinations(sorted(vertices), 2) if any(v in cover for v in e) and rng.randrange(2)}
    deleted, added = list(edits & base), list(edits - base)
    deleted.sort(); added.sort()
    rng.shuffle(deleted); rng.shuffle(added)
    run_independent_fixture_check((vertices, groups, deleted, added, cover), "randomized")

# Sparse matching, shared cancellation, retained unions, dense negatives,
# all-seed floor, inserted outside bridge, global overlap, and empty universe.
targeted = [
    ((0, 1, 2, 3), ((0, 1, 2, 3),), ((0, 1), (2, 3)), (), (0, 1, 2, 3)),
    (tuple(range(8)), ((0, 1), (0, 2), (0, 1), (0, 3, 4, 5, 6, 7)),
     tuple((0, v) for v in range(3, 8)), (), (0, 1, 2)),
    (tuple(range(7)), ((0, 1, 2), (1, 3, 4, 5, 6)),
     ((0, 2), (1, 3), (1, 4), (1, 5), (1, 6)), (), (0, 1, 2)),
    (tuple(range(4)), (tuple(range(4)),) * 3, tuple(combinations(range(4), 2)), (), tuple(range(4))),
    (tuple(range(5)), (tuple(range(5)),), ((0, 1), (2, 3), (0, 4), (1, 4)), (), (0, 1, 2, 3)),
    ((9, 0, 5, 101), (), (), ((0, 5), (0, 9)), (5, 9)),
    ((0, 1, 2), ((0, 1, 2), (0, 1, 2)), ((0, 1), (0, 2)), (), (0, 1)),
    ((), ((),), (), (), ()),
]
injection_counts = Counter()
for case in targeted:
    source, _ = run_independent_fixture_check(case, "targeted")
    print("TARGET", case, "events", dict(source.expected_counts),
          "caps", source.expected_caps, "fallback_postings", source.receipt["fallback_core_posting_reads"],
          "fallback_candidates", len(source.fallback_pairs))
    for index, (phase, operation) in enumerate(source.operations, 1):
        if phase not in ("local", "fallback"):
            continue
        for error_type in (InjectedSourceReadFailure, ValueError, StopIteration):
            failing = IndependentPreparedSourceFixture(*case, fail_at=index, error_type=error_type)
            output = []
            try:
                lead.run_prepared_fault_connectivity(
                    failing, case[4], lambda *row: output.append(row),
                    max_state_nodes=failing.factor_count + len(case[4]), core_plan="capped")
            except error_type as error:
                assert str(error).startswith("injected ")
            else:
                raise AssertionError(("source failure swallowed", case, index, error_type))
            assert output == [], "labels emitted after source failure"
            assert all(c.closed for c in failing.opened), "owned cursor not closed"
            if phase == "local":
                assert not failing.receipt["fallback_passes"], "source failure treated as cap"
            injection_counts[(phase, operation, error_type.__name__)] += 1

for metadata in (lambda n: n + 1, lambda n: -1, lambda n: True):
    case = targeted[0]
    source = IndependentPreparedSourceFixture(*case, metadata=metadata)
    output = []
    try:
        lead.run_prepared_fault_connectivity(source, case[4], lambda *row: output.append(row),
            max_state_nodes=5, core_plan="capped")
    except ValueError as error:
        assert "row length" in str(error)
    else:
        raise AssertionError("bad row metadata accepted")
    assert not output and all(c.closed for c in source.opened)
    totals["metadata_rejections"] += 1

print("COUNTS", dict(sorted(totals.items())))
print("CAP_BOUNDARIES", dict(sorted(boundaries.items())))
print("FAILURE_INJECTIONS", sum(injection_counts.values()))
print("INJECTION_SURFACES", sorted({(p, op) for p, op, error in injection_counts}))
for index, case in enumerate(targeted):
    source, _ = run_independent_fixture_check(case, "complement_targeted", plan="complement")
    print("COMPLEMENT_TARGET", index, source.receipt["local_search_events"])
cover, outside = tuple(range(13)), tuple(range(100, 200))
replay = (cover + outside, tuple((0, v) for v in cover[1:]) + ((0,) + outside,),
          tuple((0, v) for v in outside), (), cover)
for plan in ("capped", "complement"):
    source, _ = run_independent_fixture_check(replay, "replay_" + plan, plan=plan)
    print("REPLAY", plan, source.receipt["local_search_events"],
          source.receipt["fallback_core_posting_reads"], source.receipt["fallback_passes"])
print("FINAL_COUNTS", dict(sorted(totals.items())))
print("PASS independent labels, union soundness, exact event prefixes, shared fallback, failure propagation")
```

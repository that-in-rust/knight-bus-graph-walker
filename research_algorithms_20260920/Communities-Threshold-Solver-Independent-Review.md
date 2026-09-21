# Independent Review: Proposal-Free Two-Community Threshold Solver

Date: 2026-09-21. Independent mathematical review, not an implementation audit.

## Scope And Verdict

Repository evidence read: `Communities-Proposal-Free-Threshold-Solver.md` and
its linked `Communities-Single-Flip-Trace-Verification.md`, only. No candidate
implementation, lead fixtures, receipts, native snapshots, or previous review
checker were read/imported. This file is the only owned output. Public primary
literature is compared below. The embedded standard-library checker is an
independently written executable mathematical model, not a production solver
or a replacement cached-scalar benchmark.

The mathematical lenses are objective algebra, ordered range search, resumable
state machines, and adversarial falsification. The alternatives considered are
literal objective replay (oracle), cached scalar visits (the lead's comparator),
ordered threshold certificates (the reviewed algorithm), and gain-priority or
neighbor-frontier execution (not trajectory-equivalent in general).

**Verdict:** the exact two-label event-discovery theorem and its arithmetic
operation bound hold under the stated preconditions. Repeated movers are legal.
An empty suffix is not a terminal sweep. A handoff's positive pending action
makes the preceding `changed` bit redundant *if that action is actually executed
on resume*. That is not a general checkpoint theorem, nor does it specify a
general-community fallback, total budget accounting, or complete output history.

## 1. Objective And Leaf Contract

Let `T = sum_u d_u = 2 sum_edges w > 0`, `gamma > 0`, and let `e_C` count each
internal undirected edge once. The independent oracle uses

```text
Q(labels) = sum_C [2 e_C/T - gamma (vol_C/T)^2].
```

It recomputes this objective for each candidate labeling, without using gain
formulas, thresholds, the tree, or cached neighbor affinities. Subtracting the
old objective proves the manuscript's `Delta Q = 2 G/T^2` identities: removal
from the source changes its squared volume by `-2 vol_source d + d^2`;
insertion changes the destination by `2 vol_dest d + d^2`. Fresh has initial
volume zero. The edge contribution changes by twice the destination affinity
minus twice the source affinity, divided by T.

For P, both gains decrease with z; for Q, both increase. Dividing by positive
`gamma*d` gives exactly the manuscript's `L=max(two lower bounds)` and
`U=min(two upper bounds)`. Thus `L<=z<=U` is equivalent to *both* moves having
nonpositive gain. Missing bounds are infinities, not zero. An isolate has two
zero gains and no bound; never divide by its degree. Equality is safe. A
positive existing/fresh tie is an existing-community move. An unsafe leaf
identifies a positive action, not its destination: compare both gains there.

Nonempty means vertex count, not positive volume. An isolated vertex can keep
a zero-volume community nonempty. The theorem excludes loops, parallel stored
edges unless normalized first, nonpositive weights/gamma, and T=0. Validation
and normalization costs are not established by this review.

## 2. First Violating Suffix In O(log n)

Use a balanced binary tree with `N=2^ceil(log2(n))` leaves and height h. Every
node stores maximum P lower bound and minimum Q upper bound; padded leaves
are unconstrained. A node is safe iff every represented real leaf is safe.

The search below holds z fixed throughout ONE query. Reject intervals wholly
before the cursor; reject safe nodes; otherwise descend left before right,
stopping immediately on the first real leaf. Partial overlap alone does not
permit accepting a prefix violation as a suffix event.

**Proof of work bound:** an expanded unsafe internal node either straddles
the suffix boundary or lies on the returned leaf's ancestor path. A wholly
included unsafe interval contains a violating leaf and cannot be exhaustively
searched without returning one. There is at most one boundary-straddling node
per level and one returned-leaf ancestor per level. Hence at most 2h internal
expansions, and at most `4h+1` entered nodes, including rejected children. If
there is no result, only the boundary path expands: at most `2h+1` entries.
At cursor zero, a no-result query examines just the safe root. These are
conservative bounds for this precise traversal, not measured candidate counts.

An alternative proof decomposes the suffix into O(h) canonical intervals,
checks each once, and descends once into the first unsafe interval. Binary
searching ranks with a fresh O(log n) range query each step instead costs
O(log^2 n); binary searching individual leaf safety is invalid because the
leaf predicate need not be monotone. Enumerating every violating leaf loses
the first-event bound. An unbalanced tree does not have height O(log n).

## 3. Event Order, Updates, Repetition, Termination

Induct on accepted events. Initially the labels, affinities, scalar z and
leaves are exact. Search returns the earliest profitable rank at or after the
cursor. All intervening visits stay, so the state and z remain unchanged;
therefore the serial executor reaches the same state and chooses the same
best positive action, with the same exact gain.

For a supported move, change the mover's label, add/subtract its degree to z,
and add/subtract its incident weights to every neighbor's Q affinity. Its own
affinity is unchanged because the graph is loopless, but its leaf must be
recomputed for its NEW label. Update already visited neighbors as well. Set
the cursor to the moved rank plus one, not the moved rank or zero. Nonneighbors'
affinities do not change; their gains can change via z. Every subsequent query
uses current z. These facts reestablish the invariant without forbidding later
moves by the same vertex.

No event in the suffix certifies only that suffix. If this sweep already
changed, begin the next sweep at rank zero with `changed=false`. If it did not
change, the skipped prefix and suffix together certify a whole no-move sweep.
Only then return `complete`, including that terminal sweep number. Even if a
post-move state is globally stable, the changed sweep is not the terminal
no-move sweep under the specified contract.

Strict objective increase forbids a repeated partition, not a repeated mover.
Indeed Q is invariant under exchanging P and Q. There are at most
`2^(n-1)-1` unordered nonempty bipartitions, giving the stronger but still
exponential bound `f <= 2^(n-1)-2` on supported accepted events from a fresh
two-label start. Distinct states with equal Q cannot both occur on the strict
trajectory. This is an upper bound, not a construction attaining it or a
polynomial deadline. The old `J<=2M` argument cannot be inherited from a
single-flip verifier: J counts adjacency records again on every repeated move.

## 4. Handoff And Resume Contract

At the earliest profitable pending visit, choose the best destination first.
Use this precedence, consistent with "before the next supported positive move":

1. Strictly superior fresh gain: `fresh_destination` before executing it.
2. Otherwise the chosen switch would remove the source's last vertex:
   `empty_community` before executing it.
3. Otherwise, if the accepted-event allowance is exhausted: `move_budget`.
4. Otherwise execute the supported switch.

A tie favors the existing community even when doing so empties the source;
that requires an empty handoff, not substitution of the fresh action. Prefix
labels and objective have not changed at the pending event. Check actual
profitability before budget: budget zero may complete a no-move sweep. Reaching
exactly the budget is not itself failure when no later supported move exists.
With budget exhausted, unsupported actions still take their respective status.

**The preceding-changed question:** at all THREE documented handoffs, the
pending action is strictly positive. Given identical source, labels, gamma,
pending sweep/rank, and a fixed deterministic general-executor policy, its
choice is independent of the earlier changed bit. If resume actually accepts
that action, both possible earlier bits become true before sweep-end handling.
Thus both continuations coincide, including the terminal sweep. The checker
resumes every generated handoff with each bit and compares complete future
events, exact gains, labels and terminal sweep against uninterrupted objective
replay. It does not claim the candidate exposes such an API.

**Limits of that sufficiency:**

- A generic checkpoint at a stay, an empty suffix, or an arbitrary interruption
  DOES need the preceding changed bit, unless reconstructible from a retained
  prefix. The counterexample below demonstrates a different terminal sweep.
- Resuming with an unchanged exhausted budget must halt again, not silently
  advance past the pending action. A total cap needs the accepted count (or
  remaining allowance). Absolute sweep/ID alone does not encode that allowance.
- Forward continuation does not reconstruct earlier events/gains needed for
  complete output. Retain the verified prefix or explicitly return deltas.
- Two-label gains specify the FIRST handoff action, not the entire fallback
  after a third community exists. Define existing-community tie order, fresh
  token naming/allocation, empty-community removal, budget scope and output
  joining. The checker uses increasing numeric existing tokens, fresh last,
  and `max(current tokens)+1`; that is a test policy, not a manuscript promise.
  A policy requiring never-reused token IDs also needs its allocator state.
- Source immutability, full current label ownership, original-ID ordering and
  exact gamma are required. Persistent checkpoints also need validation and
  crash/recovery semantics. No external-state or production resume API is proved.

## 5. Exact Cost Accounting

Let f be accepted supported moves, S the sweeps entered (including terminal or
handoff sweep), and `J=sum_events deg_records(mover)`. With one suffix search
even at cursor n, the reference model makes exactly `f+S` searches, whether
complete or halted. It performs f mover-leaf and J neighbor-leaf replacements.
There are `2N-1` initialized tree nodes, `N-1` initial internal merges, and
`(f+J)(h+1)` subsequent node writes when each point update rewrites its path.
These exact counts are for the embedded model; implementations may coalesce
updates or omit empty-suffix calls.

After ranking, construction is O(n+M); search/update work is
`O(n+M+(f+S+J)log n)` arithmetic/comparison operations. For a fresh complete or
handoff run, `S<=f+1`, so the equivalent bound is
`O(n+M+(f+J+1)log n)`. Repeated moves permit `J<=f(n-1)` for a simple graph,
not a universal single-pass `2M` charge. Ranking arbitrary integer IDs adds
O(n log n) comparisons, whose bit costs matter. Source validation is additional
unless already included in the ranked-source promise.

Storage is O(n+M+f) records including graph, ranks, current state, tree and
retained events; serial streaming output changes the retention contract.
Exact rational arithmetic, denominator normalization, comparisons and ID
widths are not unit-time physical operations. Dense high-degree movement can
make logarithmic tree updates worse than a competent cached scalar loop.
This review reports only logical counts, no timing, RSS, speedup, low-RAM cap,
native-input admission rate, or evidence about the lead's implementation.

## 6. Independent Evidence

The final embedded checker passed with assertions enabled and warnings treated
as errors. It imports only Python standard-library modules and writes no files.
Counts below are independently reproduced, not copied from a lead receipt.

| Coverage | Exact count |
| --- | ---: |
| Exhaustive cases: n=2,3 with edge weights absent/1/2; n=4 absent/1 | 4,168 |
| Seeded n=5..8 rational-weight cases, seed 202609210731 | 320 |
| Tiny-margin boundary cases / fixed adversarial cases | 3 / 5 |
| Total unbudgeted cases | 4,496 |
| Complete / empty-community / fresh-destination outcomes | 1,693 / 1,294 / 1,509 |
| Cases whose supported prefix includes repeated movers | 19 |
| Budget-prefix comparisons, every allowance 0 through f+1 | 10,981 |
| Budget outcomes: complete / empty / fresh / move-budget | 3,386 / 2,588 / 3,018 / 1,989 |
| Positive-pending continuation comparisons, both changed bits | 15,190 |
| Re-halts with unchanged exhausted allowance | 1,989 |
| Direct objective gain / leaf-safety identities | 36,158 / 18,079 |
| Exhaustive Boolean-violation suffix queries, n=1..10 | 20,480 |
| Single-offender queries, n=31,32,33,64,65,129 | 28,390 |
| Nodes entered by exhaustive Boolean-violation queries | 135,074 |
| Explicit faulty solver variants detected | 5 |

The exhaustive family contains 16 + 624 + 3,528 cases. It covers every
nonempty graph assignment in those alphabets, every nonconstant binary label
vector and gamma in `{1/2,1,2,3}`; it is not exhaustive over rational inputs.
Rank tests cover all cursors, equality, real and padded boundaries, first and
last ranks, unsafe excluded prefixes, no result, and nonmonotone leaf safety.
An additional roundtrip uses unsorted original IDs from `-2^521` to `2^521`.
This is a reference rank-mapping check, not a test of candidate serialization.

Exact unbudgeted reference-model totals, summed over all 4,496 cases:

| Charged unit | Count |
| --- | ---: |
| Supported accepted events f / sweeps entered S | 1,989 / 5,639 |
| Objective-oracle serial visits (includes pending visit on handoff) | 16,373 |
| Searches f+S / entered search nodes | 7,628 / 20,685 |
| Neighbor-affinity updates J | 3,736 |
| Mover-plus-neighbor leaf replacements f+J | 5,725 |
| Point-update tree nodes written, excluding build | 18,354 |
| Initialized tree nodes / initial internal merges | 33,992 / 14,748 |
| Leaf-threshold evaluations, including initialization | 23,804 |

The *whole checker*, including repeated oracles, budget/resume tests and
witness checks, makes 841,152 full objective evaluations. That deliberately
expensive verification work is not a competent baseline. These are distinct
logical units, not interchangeable operation weights, timings, simultaneous
storage, or speedup ratios. The user subsequently reported native/fallback
results and a larger point-update cost than cached-visit count; those separate
lead results were not independently inspected or incorporated into these counts.

### Concrete Counterexamples

Vertices below are increasing ranks, labels are binary vectors (`0=P,1=Q`),
edges `uv:w` occur once, and all gains listed are actual Delta Q.

1. **Repeated movers; J can exceed 2M.** Five vertices, edges `03:1,23:1,34:5`,
   labels `01010`, gamma `3/4`. The exact accepted sequence is
   `(s1,0->Q,53/392), (s1,2->Q,47/392), (s1,3->P,15/56),
   (s2,0->P,23/392), (s2,2->P,17/392)`. Terminal sweep is 3, labels `01000`.
   Vertices 0 and 2 each move twice; J=7 and 2M=6. Both permanent mover locking
   and omitted already-visited neighbor updates fail on this case.
2. **A neighbor-only frontier misses an actual next event.** Five vertices,
   edges `03:2,12:1`, labels `01110`, gamma `4/3`. Rank 0 switches to Q with
   gain `2/27`. Nonneighbor rank 1 previously had gains `(-7/27,-1/9)` for
   `(switch,fresh)`; afterward both are `1/27`. Its affinity did not change.
   It is the very next accepted move, to P by the existing-token tie rule.
3. **A positive switch is not enough.** Four vertices, edges
   `01:1,02:1,13:1,23:1`, labels `0010`, gamma 3. Rank 0's switch gains `3/8`,
   but fresh gains `1/2`: hand off before any move, even with budget zero.
   Ignoring fresh entirely also fails on three vertices, edges `02:1,12:1`,
   labels `010`, gamma 3: rank 0 has switch `-1/8` and fresh `1/4`.
4. **Gain priority changes event order.** Four vertices, edges `01:1,23:2`,
   labels `0101`, gamma 1. Ranks 0 and 2 initially offer `5/18` and `4/9`.
   The required first mover is 0, not the larger-gain rank 2.
5. **Exact sign, tie, zero volume, and termination.** Three vertices, only
   edge `01:1`, labels `001`, gamma `2+epsilon`. At rank 0 both gains are
   `epsilon/2`. With epsilon `-10^-80` or zero, complete at sweep 1 even with
   budget zero. With `+10^-80`, move to existing Q (which was nonempty but
   zero-volume), or return move-budget if allowance zero. With allowance one,
   complete at sweep 2. Binary floating-point rounds these gamma values to
   the same value. An epsilon-based stay or a fresh-on-positive-tie rule fails.
6. **Generic changed-bit loss.** Use the preceding graph with gamma 3. At
   `(sweep=1,next_rank=2)`, labels `101` can result from moving rank 0 earlier
   in the sweep, or from a fresh run initialized at `101` with only stays.
   The identical remaining stay produces terminal sweep 2 in the first case,
   1 in the second. Thus a generic checkpoint is not covered by the
   positive-pending handoff sufficiency theorem.
7. **Late rank and empty suffix.** Six vertices, edges
   `03:6,04:6,12:6,15:1,34:5`, labels `011000`, gamma 2. Only rank 5 moves
   in sweep 1, to Q with gain `5/64`; the terminal sweep is 2. Returning
   complete on that now-empty suffix misreports the required terminal sweep.
8. **Empty before budget.** Two vertices with edge `01:1`, labels `01`,
   gamma 1: rank 0's switch has gain `1/2`, fresh zero. With budget zero the
   status is empty-community at `(1,0)`, labels unchanged and no accepted moves.

The five executed solver mutations are `switch_only`, `suffix_terminal`,
`fresh_tie`, `omit_past_neighbors`, and `permanent_lock`. The checker finds and
prints independent witnesses for each. Separate executable counterexamples
cover neighbor-only activation, gain-priority order, the single-flip J bound,
nonempty-versus-positive-volume, tolerance, generic changed state, budget
precedence and exact-budget completion. Boolean pattern `[unsafe,safe,unsafe]`
also refutes binary search on individual leaf safety. This is bounded
falsification, not mutation coverage of a native implementation.

## 7. Bounded Prior-Art Comparison

- Kernighan-Lin optimizes balanced graph cuts using selected exchanges and a
  best improving prefix of a pass. It is not this fixed increasing-ID,
  positive-single-move trace. Exchange refinement and maintaining move gains
  are prior art, not proposed novelty. Primary paper:
  [Kernighan and Lin (1970)](https://www.cs.princeton.edu/~bwk/btl.mirror/partitioning.pdf).
- Fiduccia-Mattheyses uses individual moves, gain buckets, local gain updates,
  pass locking and the best partition encountered in a pass. Its linear
  per-pass result concerns its network/cut and balance model, not unbounded
  exact rational modularity thresholds or this visit schedule. Repeated
  passes are established; permanent single-flip locking here would change
  the contract. Primary paper:
  [Fiduccia and Mattheyses (1982)](https://limsk.ece.gatech.edu/book/papers/fm.pdf).
- Kinetic data structures maintain certificates and repair them at failure
  events. The certificate/event viewpoint is established. Here z jumps in
  either direction at chosen discrete moves, and events are ordered by visit
  rank, not a continuously predicted physical failure time. An ordinary
  neighbor-only active set misses globally activated nonneighbors; a stronger
  certificate-aware active set could match the reduction. Primary sources:
  [Basch, Guibas and Hershberger](https://graphics.stanford.edu/courses/cs268-11-spring/notes/kinetic.pdf),
  [Basch et al., Animating Proofs](https://graphics.stanford.edu/~comba/papers/socg.pdf).

These are distinctions of contracts, not measured comparisons or exclusions
of all optimized variants. No priority conclusion follows from absent search
hits. The narrower candidate contribution is a useful, exact two-label
modularity reduction preserving a prescribed serial trajectory; novelty and
native usefulness remain open. This review duplicates neither the lead's
cached scalar control nor its native snapshot warm-start gate.

## 8. Reproduction

Run from the repository root; this extracts only the Python block and writes
no files. Python assertions must remain enabled (do not use `-O`).

```sh
awk '/^<!-- THRESHOLD-SOLVER-REVIEW-CHECKER-START -->$/{p=1;next} /^<!-- THRESHOLD-SOLVER-REVIEW-CHECKER-END -->$/{exit} p && !/^```/{print}' research_algorithms_20260920/Communities-Threshold-Solver-Independent-Review.md | python3 -B -W error
```

<!-- THRESHOLD-SOLVER-REVIEW-CHECKER-START -->
```python
from collections import Counter
from fractions import Fraction as F
from itertools import combinations, product
import json
from random import Random


COUNTS = Counter()
WITNESSES = {}


def compute_partition_objective_exact(graph, labels, gamma):
    COUNTS["objective_evaluations"] += 1
    n, edges = graph
    degrees = [F(0) for _ in range(n)]
    internal = F(0)
    for u, v, w in edges:
        degrees[u] += w
        degrees[v] += w
        if labels[u] == labels[v]:
            internal += w
    total = sum(degrees)
    assert total > 0
    volumes = {}
    for u, label in enumerate(labels):
        volumes[label] = volumes.get(label, F(0)) + degrees[u]
    return 2 * internal / total - gamma * sum(v * v for v in volumes.values()) / total**2


def enumerate_objective_candidate_gains(graph, labels, gamma, u):
    old = compute_partition_objective_exact(graph, labels, gamma)
    existing = sorted(set(labels) - {labels[u]})
    destinations = existing + [max(labels) + 1]
    gains = []
    for dest in destinations:
        trial = list(labels)
        trial[u] = dest
        gains.append((dest, compute_partition_objective_exact(graph, trial, gamma) - old))
    return gains


def select_objective_positive_move(graph, labels, gamma, u):
    gains = enumerate_objective_candidate_gains(graph, labels, gamma, u)
    dest, gain = max(gains, key=lambda item: item[1])
    return (dest, gain) if gain > 0 else None


def replay_objective_serial_visits(graph, initial, gamma, budget=None,
                                   checkpoint=(1, 0, False), general=False):
    labels = list(initial)
    sweep, cursor, changed = checkpoint
    events = []
    visits = 0
    while True:
        for u in range(cursor, graph[0]):
            visits += 1
            choice = select_objective_positive_move(graph, labels, gamma, u)
            if choice is None:
                continue
            dest, gain = choice
            reason = None
            if not general and dest not in labels:
                reason = "fresh_destination"
            elif not general and labels.count(labels[u]) == 1:
                reason = "empty_community"
            elif budget is not None and len(events) == budget:
                reason = "move_budget"
            if reason:
                return (reason, tuple(labels), tuple(events), sweep, (sweep, u)), visits
            events.append((sweep, u, dest, gain))
            labels[u] = dest
            changed = True
        if not changed:
            return ("complete", tuple(labels), tuple(events), sweep, None), visits
        sweep, cursor, changed = sweep + 1, 0, False


def compute_current_threshold_state(graph, labels):
    n, edges = graph
    degrees, affinities = [F(0)] * n, [F(0)] * n
    adjacency = [[] for _ in range(n)]
    for u, v, w in edges:
        degrees[u] += w
        degrees[v] += w
        affinities[u] += w * (labels[v] == 1)
        affinities[v] += w * (labels[u] == 1)
        adjacency[u].append((v, w))
        adjacency[v].append((u, w))
    z = sum(d for d, label in zip(degrees, labels) if label == 1)
    return degrees, affinities, adjacency, sum(degrees), z


def compute_scaled_candidate_gains(label, d, a, total, gamma, z):
    if label == 0:
        return (total * (2*a-d) - gamma*d*(2*z-total+d),
                -total*(d-a) + gamma*d*(total-z-d))
    return (total*(d-2*a) - gamma*d*(total-2*z+d),
            -total*a + gamma*d*(z-d))


def compute_leaf_threshold_pair(label, d, a, total, gamma, switch_only=False):
    if d == 0:
        return None, None
    if label == 0:
        lo = (total*(2*a-d) + gamma*d*(total-d)) / (2*gamma*d)
        fresh = total-d-total*(d-a)/(gamma*d)
        return (lo if switch_only else max(lo, fresh)), None
    hi = (gamma*d*(total+d) - total*(d-2*a)) / (2*gamma*d)
    fresh = d + total*a/(gamma*d)
    return None, (hi if switch_only else min(hi, fresh))


def merge_interval_threshold_pairs(left, right):
    lowers = [x for x in (left[0], right[0]) if x is not None]
    uppers = [x for x in (left[1], right[1]) if x is not None]
    return (max(lowers) if lowers else None, min(uppers) if uppers else None)


def check_interval_threshold_safety(pair, z):
    lo, hi = pair
    return (lo is None or z >= lo) and (hi is None or z <= hi)


class OrderedThresholdSuffixTree:
    def __init__(self, pairs):
        self.n = len(pairs)
        self.h = (self.n-1).bit_length()
        self.size = 1 << self.h
        self.nodes = [(None, None)] * (2*self.size)
        self.nodes[self.size:self.size+self.n] = pairs
        for index in range(self.size-1, 0, -1):
            self.nodes[index] = merge_interval_threshold_pairs(self.nodes[2*index], self.nodes[2*index+1])

    def replace_rank_threshold_pair(self, rank, pair):
        index = self.size + rank
        self.nodes[index] = pair
        writes = 1
        while index > 1:
            index //= 2
            self.nodes[index] = merge_interval_threshold_pairs(self.nodes[2*index], self.nodes[2*index+1])
            writes += 1
        assert writes == self.h+1
        return writes

    def find_first_suffix_violation(self, cursor, z):
        examined = 0

        def descend_left_before_right(index, lo, hi):
            nonlocal examined
            examined += 1
            if hi <= cursor or check_interval_threshold_safety(self.nodes[index], z):
                return None
            if hi-lo == 1:
                assert lo < self.n
                return lo
            mid = (lo+hi)//2
            first = descend_left_before_right(2*index, lo, mid)
            return first if first is not None else descend_left_before_right(2*index+1, mid, hi)

        result = descend_left_before_right(1, 0, self.size)
        assert examined <= 4*self.h+1
        if result is None:
            assert examined <= 2*self.h+1
        return result, examined


def replay_threshold_ordered_events(graph, initial, gamma, budget=None, mutant=None):
    labels = list(initial)
    degrees, affinities, adjacency, total, z = compute_current_threshold_state(graph, labels)
    pairs = [compute_leaf_threshold_pair(labels[u], degrees[u], affinities[u], total, gamma,
                                        mutant == "switch_only") for u in range(graph[0])]
    tree = OrderedThresholdSuffixTree(pairs)
    counts = Counter(build_nodes=2*tree.size-1, build_merges=tree.size-1,
                     threshold_evaluations=graph[0])
    sweep, cursor, changed = 1, 0, False
    events = []
    moved = set()
    while True:
        rank, entered = tree.find_first_suffix_violation(cursor, z)
        counts["searches"] += 1
        counts["search_nodes"] += entered
        if rank is None:
            if not changed or mutant == "suffix_terminal":
                result = ("complete", tuple(labels), tuple(events), sweep, None)
                break
            sweep, cursor, changed = sweep+1, 0, False
            continue
        if mutant == "permanent_lock" and rank in moved:
            cursor = rank+1
            continue
        switch, fresh = compute_scaled_candidate_gains(labels[rank], degrees[rank], affinities[rank], total, gamma, z)
        reason = None
        if fresh > switch or (mutant == "fresh_tie" and fresh == switch):
            reason = "fresh_destination"
        elif labels.count(labels[rank]) == 1:
            reason = "empty_community"
        elif budget is not None and len(events) == budget:
            reason = "move_budget"
        if reason:
            result = (reason, tuple(labels), tuple(events), sweep, (sweep, rank))
            break
        assert switch > 0
        dest = 1-labels[rank]
        events.append((sweep, rank, dest, 2*switch/total**2))
        moved.add(rank)
        labels[rank] = dest
        direction = 1 if dest == 1 else -1
        z += direction*degrees[rank]
        replacements = [rank]
        for neighbor, weight in adjacency[rank]:
            affinities[neighbor] += direction*weight
            counts["neighbor_updates"] += 1
            if mutant != "omit_past_neighbors" or neighbor > rank:
                replacements.append(neighbor)
        for u in replacements:
            pair = compute_leaf_threshold_pair(labels[u], degrees[u], affinities[u], total, gamma,
                                               mutant == "switch_only")
            counts["tree_writes"] += tree.replace_rank_threshold_pair(u, pair)
            counts["leaf_replacements"] += 1
            counts["threshold_evaluations"] += 1
        changed, cursor = True, rank+1
        if mutant is None:
            _, truth, _, _, truth_z = compute_current_threshold_state(graph, labels)
            assert affinities == truth and z == truth_z
            for u in range(graph[0]):
                expected = compute_leaf_threshold_pair(labels[u], degrees[u], truth[u], total, gamma)
                assert tree.nodes[tree.size+u] == expected
    if mutant is None:
        assert counts["searches"] == len(events)+sweep
        assert counts["leaf_replacements"] == len(events)+counts["neighbor_updates"]
        assert counts["tree_writes"] == counts["leaf_replacements"]*(tree.h+1)
    return result, counts


def retain_first_named_witness(name, graph, labels, gamma, result=None):
    if name not in WITNESSES:
        WITNESSES[name] = (graph, tuple(labels), gamma, result)


def check_case_objective_identities(graph, initial, gamma):
    degrees, affinities, _, total, z = compute_current_threshold_state(graph, initial)
    for u in range(graph[0]):
        actual = enumerate_objective_candidate_gains(graph, initial, gamma, u)
        switch, fresh = compute_scaled_candidate_gains(initial[u], degrees[u], affinities[u], total, gamma, z)
        assert [gain for _, gain in actual] == [2*switch/total**2, 2*fresh/total**2]
        COUNTS["gain_identities"] += 2
        pair = compute_leaf_threshold_pair(initial[u], degrees[u], affinities[u], total, gamma)
        assert check_interval_threshold_safety(pair, z) == (max(gain for _, gain in actual) <= 0)
        COUNTS["threshold_identities"] += 1


def check_case_complete_contract(graph, initial, gamma, family):
    COUNTS[family] += 1
    check_case_objective_identities(graph, initial, gamma)
    expected, visits = replay_objective_serial_visits(graph, initial, gamma)
    found, counts = replay_threshold_ordered_events(graph, initial, gamma)
    assert found == expected, (graph, initial, gamma, expected, found)
    COUNTS["unbudgeted_"+expected[0]] += 1
    COUNTS["unbudgeted_scalar_visits"] += visits
    COUNTS["unbudgeted_events"] += len(expected[2])
    for name, value in counts.items():
        COUNTS["model_"+name] += value
    if len({e[1] for e in expected[2]}) < len(expected[2]):
        COUNTS["repeated_mover_cases"] += 1
        retain_first_named_witness("repeated", graph, initial, gamma, expected)
    if expected[3] >= 3 and expected[0] == "complete":
        retain_first_named_witness("late_sweep", graph, initial, gamma, expected)
    for budget in range(len(expected[2])+2):
        oracle, _ = replay_objective_serial_visits(graph, initial, gamma, budget)
        model, _ = replay_threshold_ordered_events(graph, initial, gamma, budget)
        assert oracle == model
        COUNTS["budget_comparisons"] += 1
        COUNTS["budget_"+model[0]] += 1
        if model[0] == "complete":
            continue
        status, labels, prefix, sweep, pending = model
        assert pending[0] == sweep
        choice = select_objective_positive_move(graph, labels, gamma, pending[1])
        assert choice is not None
        uninterrupted, _ = replay_objective_serial_visits(graph, initial, gamma, general=True)
        for changed in (False, True):
            continued, _ = replay_objective_serial_visits(graph, labels, gamma,
                                                         checkpoint=(sweep, pending[1], changed), general=True)
            joined = (continued[0], continued[1], prefix+continued[2], continued[3], continued[4])
            assert joined == uninterrupted
            COUNTS["resume_comparisons"] += 1
        if status == "move_budget":
            repeated, _ = replay_objective_serial_visits(graph, labels, gamma, budget=0,
                                                         checkpoint=(sweep, pending[1], False))
            assert repeated[0] == "move_budget" and repeated[4] == pending and not repeated[2]
            COUNTS["still_exhausted_resumes"] += 1
    for mutant in ("switch_only", "suffix_terminal", "fresh_tie", "omit_past_neighbors", "permanent_lock"):
        if mutant in WITNESSES:
            continue
        try:
            wrong, _ = replay_threshold_ordered_events(graph, initial, gamma, mutant=mutant)
        except AssertionError:
            wrong = "invalid-positive-event"
        if wrong != expected:
            retain_first_named_witness(mutant, graph, initial, gamma, expected)


def exercise_exhaustive_graph_families():
    for n in (2, 3, 4):
        pairs = list(combinations(range(n), 2))
        weights = (0, 1, 2) if n < 4 else (0, 1)
        for vector in product(weights, repeat=len(pairs)):
            if not any(vector):
                continue
            graph = n, tuple((u, v, F(w)) for (u, v), w in zip(pairs, vector) if w)
            for labels in product((0, 1), repeat=n):
                if len(set(labels)) != 2:
                    continue
                for gamma in (F(1, 2), F(1), F(2), F(3)):
                    check_case_complete_contract(graph, labels, gamma, "exhaustive_cases")


def exercise_seeded_rational_graphs():
    rng = Random(202609210731)
    for _ in range(320):
        n = rng.randrange(5, 9)
        edges = tuple((u, v, F(rng.randrange(1, 10), rng.randrange(1, 8)))
                      for u, v in combinations(range(n), 2) if rng.randrange(3) != 0)
        if not edges:
            edges = ((0, 1, F(1)),)
        labels = (0, 1) + tuple(rng.randrange(2) for _ in range(n-2))
        gamma = F(rng.randrange(1, 10), rng.randrange(1, 6))
        check_case_complete_contract((n, edges), labels, gamma, "seeded_cases")


def exercise_exhaustive_rank_searches():
    for n in range(1, 11):
        for bits in product((False, True), repeat=n):
            pairs = [(F(1) if bit else F(0), None) for bit in bits]
            tree = OrderedThresholdSuffixTree(pairs)
            for cursor in range(n+1):
                expected = next((u for u in range(cursor, n) if bits[u]), None)
                found, entered = tree.find_first_suffix_violation(cursor, F(0))
                assert found == expected
                COUNTS["exhaustive_rank_queries"] += 1
                COUNTS["exhaustive_rank_nodes"] += entered
    # Non-power-of-two padding, every possible first/last position and cursor.
    for n in (31, 32, 33, 64, 65, 129):
        for offender in range(n):
            pairs = [(None, None)] * n
            pairs[offender] = (None, F(-1))
            tree = OrderedThresholdSuffixTree(pairs)
            for cursor in range(n+1):
                found, _ = tree.find_first_suffix_violation(cursor, F(0))
                assert found == (offender if offender >= cursor else None)
                COUNTS["adversarial_rank_queries"] += 1
    tree = OrderedThresholdSuffixTree([(F(1), None), (F(0), None), (F(1), None)])
    assert [tree.find_first_suffix_violation(c, F(0))[0] for c in range(4)] == [0, 2, 2, None]
    COUNTS["unsafe_leaf_binary_search_exposed"] += 1


def exercise_exact_boundary_contracts():
    graph = (3, ((0, 1, F(1)),))
    labels = (0, 0, 1)
    epsilon = F(1, 10**80)
    for sign in (-1, 0, 1):
        gamma = F(2) + sign*epsilon
        gains = enumerate_objective_candidate_gains(graph, labels, gamma, 0)
        assert [gain for _, gain in gains] == [sign*epsilon/2]*2
        expected, _ = replay_objective_serial_visits(graph, labels, gamma, budget=0)
        assert expected[0] == ("move_budget" if sign > 0 else "complete")
        check_case_complete_contract(graph, labels, gamma, "boundary_cases")
    assert float(F(2)+epsilon) == float(F(2)-epsilon)
    COUNTS["unsafe_tolerance_exposed"] += 1
    degrees, _, _, _, z = compute_current_threshold_state(graph, labels)
    assert z == 0 and labels.count(1) == 1 and degrees[2] == 0
    positive, _ = replay_objective_serial_visits(graph, labels, F(3))
    assert positive[2][0][1:3] == (0, 1)
    COUNTS["unsafe_nonempty_volume_exposed"] += 1
    edge = (2, ((0, 1, F(1)),))
    empty, _ = replay_objective_serial_visits(edge, (0, 1), F(1), budget=0)
    assert empty[0] == "empty_community" and empty[4] == (1, 0) and not empty[2]
    COUNTS["empty_before_budget_checks"] += 1
    # A generic checkpoint after a real move, unlike a positive-pending handoff.
    stable = positive[1]
    assert positive[0] == "complete"
    false_tail, _ = replay_objective_serial_visits(graph, stable, F(3), checkpoint=(1, 2, False))
    true_tail, _ = replay_objective_serial_visits(graph, stable, F(3), checkpoint=(1, 2, True))
    assert false_tail[3] == 1 and true_tail[3] == 2
    assert false_tail[1:3] == true_tail[1:3]
    COUNTS["generic_changed_flag_counterexamples"] += 1
    exact_budget, _ = replay_objective_serial_visits(graph, labels, F(3), budget=1)
    assert exact_budget == positive
    COUNTS["exact_budget_completion_checks"] += 1


def exercise_fixed_adversarial_graphs():
    # Chosen independently, then frozen; these are challenges, not a prevalence sample.
    repeat = (5, ((0, 3, F(1)), (2, 3, F(1)), (3, 4, F(5))))
    repeat_labels = (0, 1, 0, 1, 0)
    result, counts = replay_threshold_ordered_events(repeat, repeat_labels, F(3, 4))
    assert result == ("complete", (0, 1, 0, 0, 0),
                      ((1, 0, 1, F(53, 392)), (1, 2, 1, F(47, 392)),
                       (1, 3, 0, F(15, 56)), (2, 0, 0, F(23, 392)),
                       (2, 2, 0, F(17, 392))), 3, None)
    assert counts["neighbor_updates"] == 7 > 2*len(repeat[1])
    for mutant in ("permanent_lock", "omit_past_neighbors"):
        try:
            wrong, _ = replay_threshold_ordered_events(repeat, repeat_labels, F(3, 4), mutant=mutant)
        except AssertionError:
            wrong = "invalid-positive-event"
        assert wrong != result
    COUNTS["unsafe_single_flip_J_bound_exposed"] += 1

    activation = (5, ((0, 3, F(2)), (1, 2, F(1))))
    activation_labels = (0, 1, 1, 1, 0)
    old = enumerate_objective_candidate_gains(activation, activation_labels, F(4, 3), 1)
    after = (1, 1, 1, 1, 0)
    new = enumerate_objective_candidate_gains(activation, after, F(4, 3), 1)
    assert [gain for _, gain in old] == [F(-7, 27), F(-1, 9)]
    assert [gain for _, gain in new] == [F(1, 27), F(1, 27)]
    result, _ = replay_objective_serial_visits(activation, activation_labels, F(4, 3))
    assert result[2][:2] == ((1, 0, 1, F(2, 27)), (1, 1, 0, F(1, 27)))
    assert all({u, v} != {0, 1} for u, v, _ in activation[1])
    COUNTS["unsafe_neighbor_frontier_exposed"] += 1

    fresh = (4, ((0, 1, F(1)), (0, 2, F(1)), (1, 3, F(1)), (2, 3, F(1))))
    fresh_labels = (0, 0, 1, 0)
    assert [gain for _, gain in enumerate_objective_candidate_gains(fresh, fresh_labels, F(3), 0)] == [F(3, 8), F(1, 2)]
    fresh_result, _ = replay_threshold_ordered_events(fresh, fresh_labels, F(3), budget=0)
    assert fresh_result == ("fresh_destination", fresh_labels, (), 1, (1, 0))
    COUNTS["fresh_before_budget_checks"] += 1
    COUNTS["unsafe_any_positive_switch_exposed"] += 1

    priorities = (4, ((0, 1, F(1)), (2, 3, F(2))))
    priority_labels = (0, 1, 0, 1)
    first = select_objective_positive_move(priorities, priority_labels, F(1), 0)
    larger = select_objective_positive_move(priorities, priority_labels, F(1), 2)
    assert first == (1, F(5, 18)) and larger == (1, F(4, 9))
    result, _ = replay_threshold_ordered_events(priorities, priority_labels, F(1))
    assert result[2][0] == (1, 0, 1, F(5, 18))
    COUNTS["unsafe_gain_priority_exposed"] += 1

    late = (6, ((0, 3, F(6)), (0, 4, F(6)), (1, 2, F(6)),
                (1, 5, F(1)), (3, 4, F(5))))
    late_labels = (0, 1, 1, 0, 0, 0)
    late_result, _ = replay_threshold_ordered_events(late, late_labels, F(2))
    assert late_result == ("complete", (0, 1, 1, 0, 0, 1), ((1, 5, 1, F(5, 64)),), 2, None)
    ids = (-(1 << 521), -17, 0, 6, 99, 1 << 521)
    supplied = dict(reversed(list(zip(ids, late_labels))))
    ranked = sorted(supplied)
    index = {original: rank for rank, original in enumerate(ranked)}
    original_edges = tuple((ids[u], ids[v], w) for u, v, w in late[1])
    rebuilt = (len(supplied), tuple((index[u], index[v], w) for u, v, w in original_edges))
    rebuilt_labels = tuple(supplied[original] for original in ranked)
    assert rebuilt == late and rebuilt_labels == late_labels
    assert ranked[late_result[2][0][1]] == 1 << 521
    COUNTS["sparse_original_ID_roundtrips"] += 1

    for graph, labels, gamma in ((repeat, repeat_labels, F(3, 4)),
                                 (activation, activation_labels, F(4, 3)),
                                 (fresh, fresh_labels, F(3)),
                                 (priorities, priority_labels, F(1)),
                                 (late, late_labels, F(2))):
        check_case_complete_contract(graph, labels, gamma, "fixed_cases")


def print_exact_review_receipt():
    assert COUNTS["exhaustive_cases"] == 4168
    assert COUNTS["seeded_cases"] == 320
    assert COUNTS["fixed_cases"] == 5
    assert COUNTS["repeated_mover_cases"] > 0
    assert all(name in WITNESSES for name in ("repeated", "late_sweep", "switch_only", "suffix_terminal",
                                             "fresh_tie", "omit_past_neighbors", "permanent_lock"))
    print(json.dumps(dict(sorted(COUNTS.items())), indent=2))
    print("WITNESSES")
    for name, (graph, labels, gamma, result) in sorted(WITNESSES.items()):
        print(name, "n=", graph[0], "edges=", [(u, v, str(w)) for u, v, w in graph[1]],
              "labels=", labels, "gamma=", str(gamma), "result=", result)
    print("PASS: independent exact mathematical checker; no candidate imports")


def run_independent_review_checks():
    assert __debug__
    exercise_exhaustive_rank_searches()
    exercise_exact_boundary_contracts()
    exercise_exhaustive_graph_families()
    exercise_seeded_rational_graphs()
    exercise_fixed_adversarial_graphs()
    print_exact_review_receipt()


if __name__ == "__main__":
    run_independent_review_checks()
```
<!-- THRESHOLD-SOLVER-REVIEW-CHECKER-END -->

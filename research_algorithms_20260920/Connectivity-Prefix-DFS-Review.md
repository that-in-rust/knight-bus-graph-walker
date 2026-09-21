# Independent Review: Prefix-Encoded Clique DFS

Date: 2026-09-21. Scope: preparation/control candidate only. This review owns
only this file. No lead implementation, tests, manuscript, or current edited-WCC
executor was changed; no commits were made. The lead's seven-test suite was
read, not rerun or counted as independent evidence.

## Findings First

1. **No core correctness counterexample found under the trusted-source
   contract.** Prefix membership is exactly discovery state at stable step
   boundaries, including roots consumed from factors. The emitted forest has
   no cross edges, not merely recursive-looking discovery order. Proofs below
   establish both claims independently of the finite checks.
2. **Low resident space is not exclusive to this encoding.** Native DCC DFS
   with a bounded-cache disk bitmap and the same external stack also uses
   `O(F+B)` resident words. The change exchanges mutable bitmap probes for
   immutable positioned-inverse probes. It does not eliminate the shared
   `F`-sized cursor state, nor prove lower total storage, I/O, or physical RAM.
3. **The work penalty is real and sharply quantifiable.** For positive
   incidence degree `d`, the exact degree-only worst case is
   `U(d) = d(d+3)/2 - 1`, improving `d^2` but retaining quadratic growth.
   A legal, explicitly constructed family attains the sum of these bounds.
4. **Concrete API/preflight gap:** invalid `block_frames=0` is rejected only
   after allocating factor state and calling `factor_size`. This does not
   invalidate the narrower factor-slot admission test, but full admission
   before source work is false. Validate block configuration before lines
   102-103 if that stronger guarantee is intended.
5. **Cleanup is real; atomic publication is not.** Ordinary unwinding removes
   stack scratch after output, short-write, and short-read failures. Output
   already passed to `emit` remains visible, and emission precedes the next
   fallible stack push. No complete-output transaction or physical cache cap
   is implemented or measured.

Reviewed source: [probe_prefix_cursor_dfs.py](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_prefix_cursor_dfs.py:93),
[tests](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_prefix_cursor_dfs.py:101),
and [design note](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Connectivity-Prefix-Visited-DFS.md).
Graph discovery was attempted against the existing experiment index; the new
prefix symbols were absent, so these named files were read directly.

## Exact Contract

Let `C_f` be the ordered, duplicate-free member list for factor `f`. The fixed
simple undirected graph has an edge between distinct vertices exactly when
they share some factor. Empty factors, singleton factors, and different factor
identities with equal member sets are allowed. Let `F` be the number of
factors, `n` the complete original-ID universe size, `Z = sum_f |C_f|`, and
`d_v` the number of occurrences of vertex `v`. Write `N = |{v: d_v > 0}|`.
This `d_v` is incidence degree, not expanded-graph degree.

The source must provide immutable, complete occurrence rows containing every
`(factor, position)` exactly once, in a fixed but otherwise arbitrary order.
Factor sizes, row termination, complete-ID enumeration, IDs, and frame indexes
must satisfy the admitted word/addressing model. Sparse original IDs require
a paid lookup mechanism, not an uncharged resident vertex-offset dictionary.
The complete-ID stream contains every original vertex exactly once.

The resident-space statement is conditional on bounded source and sink
working buffers. Source construction, validation, indexes, external inverse
storage, and output publication are separately paid. Arbitrarily slow or
stateful provider methods are not constant-cost access for free. No theorem
here covers mutable inputs, inconsistent inverse rows, concurrent traversal,
pair deletions/insertions, canonical labels, or prescribed root/adjacency order.

## Theorem: Iff Discovery and True DFS

**Discovery invariant.** Define
`U(p) = union_f {C_f[r]: 0 <= r < p_f}`. Immediately before each candidate
step and after its successful discovery action, the discovered positive-
incidence vertices `D` satisfy `D = U(p)`.

Initially both sets are empty. Suppose the next candidate is `v=C_f[p_f]`.
The exact inverse row makes `exists (g,r) of v: r < p_g` equivalent to
`v in U(p)`, hence to `v in D`. Evaluate this before advancing. Incrementing
`p_f` changes the union to exactly `U(p) union {v}`, even when that occurrence
is not the first position of its factor. If old, do not discover again; if
new, immediately discover/emit `v`. Thus the two sets change identically.
This is a two-direction equivalence, not merely a sufficient visited test.

Root consumption obeys the same rule. Choosing an arbitrary positive-incidence
root without consuming a corresponding occurrence would invalidate the base
of the next step. The brief interval between advancing and emission is not a
stable boundary; failures there abort the run rather than establish a valid
published forest. Incidence-free vertices are emitted separately, exactly
once. A graph isolate in singleton factors has positive incidence and belongs
to the prefix argument; `d_v=0` is not synonymous with graph degree zero.

**Parent edges and intervals.** A child is emitted only while its parent's
frame scans a factor containing both. Discovery uniqueness excludes self
parent edges. The older parent is active when the new frame is pushed, so
parent edges are acyclic and lie in the input graph. Only the top frame runs;
child execution completes before the parent resumes. Consequently execution
intervals are nested or disjoint, as in recursive DFS.

**Finish lemma.** Before a frame for `u` is popped, its occurrence iterator has
passed every incident factor, and each such factor is exhausted. Global
cursors never decrease. Every member of an exhausted factor has a consumed
occurrence and is therefore discovered. Thus every graph neighbor of `u`
has been discovered by the time `u` finishes.

**No-cross proof.** Suppose an edge joined incomparable vertices `u,v` in
the emitted forest, even across different roots. Their execution intervals
are disjoint; choose `u` to be the one that finishes first. Then `v` has not
yet been discovered when `u` finishes, contradicting the finish lemma for
their shared factor. Therefore every graph edge has ancestor-comparable
endpoints. Shared global cursors do not invalidate this argument: exhaustion
by a descendant is still exhaustion before the ancestor finishes.

All factor positions are eventually consumed; all positive-incidence vertices
are therefore emitted. The final stream adds the incidence-free vertices.
Together with valid parent edges and no cross edges, this gives one tree per
connected component. It is a true undirected DFS forest for some adjacency
ordering: order each vertex's tree children by their execution order before
remaining non-tree neighbors. With no edges to incomparable subtrees, ordinary
DFS reproduces the forest. It need not match a separately specified order.

The existing nontrivial-tree leaf bound `leaves <= F` and covered tree-edge
cut bound `cuts <= 2k+F` therefore apply to this control. They do not provide
replacement edges, Euler/minimum indexes, or an edited-WCC implementation.

## Work: Tighter and Attainable

Let `P` mean `visited_membership_checks`, excluding `None` sentinel reads.
Every factor occurrence is consumed once. Each candidate for `v` examines at
most `d_v` inverse records, immediately proving `P <= sum_v d_v^2`.

There is a sharper bound. Number the entries of one immutable inverse row
`1,...,d`, and let `r_1,...,r_d` be their order of consumption. The first
test examines all `d` entries and finds no witness. Before consumption `i+1`,
exactly the first `i` occurrences have been consumed, so the first successful
test is at row rank `min(r_1,...,r_i)`. Hence, exactly,

`P_v = d + sum_{i=1}^{d-1} min(r_1,...,r_i)`.

The minimum of `i` distinct ranks is at most `d-i+1`. For `d>0` this yields

`2d-1 <= P_v <= U(d) = d + d+(d-1)+...+2 = d(d+3)/2 - 1`.

Set `U(0)=0`. Summing gives
`2Z-N <= P <= (sum_v d_v^2 + 3Z)/2 - N <= sum_v d_v^2`.
Descending consumption ranks attain the upper bound. It remains to check
that a *legal DFS*, rather than an arbitrary occurrence permutation, can
realize them. The following family does so.

### Sharp Amplification Family

For integer `d>=3`, use vertices `t,x_1,...,x_{d-2}` and `d` ordered factors:

- `C_0 = [t]`.
- `C_i = [t,x_i,x_{i+1}]` for `1 <= i <= d-3`.
- `C_{d-2} = [t,x_{d-2}]`.
- `C_{d-1} = [x_1,t]`.

Order `t`'s inverse row by decreasing factor ID. For each `x_i`, put its
occurrence in `C_i` first and its other occurrence second. These are legal
fixed prepared rows. Start the ordinary factor-root loop at `C_0`.

The root `t` is consumed in `C_0`, rank `d` of its inverse. Its first scan
enters `C_{d-1}` and discovers `x_1` before reaching `t` there. Vertex `x_1`
scans `C_1`, consuming `t` at inverse rank `d-1`, then discovers `x_2`.
Continue through the chain. Only after the deep calls return does the scan
of `C_{d-1}` reach `t` at rank 1. Thus `t`'s ranks are exactly `d,...,1`.
Each helper is also first consumed at its second inverse position and then
at its first, costing four tests. Therefore

`n=d-1, F=d, Z=3d-4, P=U(d)+4(d-2)=(d^2+11d-18)/2`.

This attains `sum_v U(d_v)` exactly, and has `P/Z = Theta(d)`. Thus the
degree-only upper bound is sharp for this executor and source contract;
an unconditional incidence-linear work claim is false. This is not a lower
bound for every algorithm or for reordered/deduplicated representations.
Such preparation changes are possible controls, not free properties of the
supplied immutable representation. Padding with `floor(d^(3/2))` incidence-
free isolates also gives `F=o(n)` while the quadratic probe term still
dominates the bitmap baseline's `n+F+Z` logical work.

### All Occurrence Calls, Not Just P

Let `c` count components containing positive-incidence vertices, including
singleton-factor components. At an empty stack, every previously discovered
vertex has exhausted all its factors. Therefore an unread factor cannot
contain a previously discovered vertex: each outer-loop consumption is a new
root, exactly `c` times.

For a successful run of the current code, the exact count is

`membership_record_reads = P + 2Z + 2N + n - c`.

Reason: discovery tests have `P` ordinary records and `N` end sentinels;
frames read `Z` exhausted-factor records, `Z-c` candidate-iteration records,
and `N` end sentinels; the final all-ID pass makes `n` reads. Factor-member
reads equal `Z`, and factor-size calls equal `F`. The explicit reread of a
suspended frame's current occurrence is included. Logical work is
`O(n+F+Z+P)` under constant-cost provider operations. This is not a source
I/O bound: interleaved factor reads and indexed inverse probes can each miss
cache, and locating an inverse record can require additional index pages.

## External Stack: Actual Bound and Limits

In [the stack implementation](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_prefix_cursor_dfs.py:16),
write `B` for `block_frames`; each packed frame is 16 bytes. There is one
preallocated `32B`-byte buffer, not an unbounded Python frame list or recursive
call stack. Spilling writes the bottom `B` frames from a full `2B` cache,
moves the top block down, and completes the push at resident count `B+1`.
Refilling happens only when a pop empties the cache and brings back `B`
frames. Peek/replace access the resident top; the spill-file suffix is
truncated after each successful refill.

Between two data transfers there are at least `B` push/pop operations:
write-to-write needs `B` net pushes, read-to-read needs `B` net pops, and
switching direction needs at least as many. Extra alternation cannot shorten
these distances. The initial transfer also needs at least `B` operations.
Thus `q` pushes/pops cause at most `floor(q/B)` block transfers. Successful
DFS pushes and pops each of its `N` framed vertices exactly once, so data
traffic is `O(1+N/B)`, conventionally `O(Scan(n))`. Block copying likewise
has amortized constant work per push/pop. The bound also survives arbitrary
top-frame replacements. A one-block cache without hysteresis would not have
this boundary-oscillation argument.

These are algorithmic block transfers when the chosen `16B`-byte chunk is
comparable to an external-memory block. File open/close, seek/truncate,
filesystem allocation, writeback, and OS page-cache behavior are separate
physical costs, not measured device-I/O counts. Peak logical spill-file
length is at most `16h <= 16N` bytes, where `h` is maximum stack depth.
Together with streamed `O(n)` output this is `O(n)` scratch/output, excluding
the paid source. A single clique forces `h=n` for any true DFS forest.

The executor's packed state is two `F`-entry `array('Q')` arrays plus the
`32B`-byte frame buffer, approximately `16F+32B` payload bytes on this
8-byte-Q platform, plus bounded interpreter/counter overhead. There is no
vertex-wide discovery/parent array in the executor. The fixture providers
and collectors do retain vertex-sized data; they are not evidence for a
whole-process bound. There is no physical memory reservation, aggregate
source/cache/sink cap, full-copy measurement, disk-space quota, or crash-
recovery guarantee. The two tuning knobs are not a process-memory cap.

## Required Disk-Bitmap Control

Use the same native factor/member access, ordinary factor-ID inverse rows,
global factor cursors/sizes, bounded external frame stack, and streamed
output. Store discovery bits on disk with a bounded page cache. Both
executors then have `O(F+B)` resident words; constant numbers of separately
bounded buffers can be included in that budget. No `n`-resident flags are
required by either. Dense ID addressing, or an external original-ID-to-dense-
rank view, must be paid consistently for the bitmap control.

| Additive visited-state cost | Prefix encoding | Disk bitmap |
|---|---|---|
| Common `F` cursors/sizes | Already needed by DCC scan | Already needed by DCC scan |
| Extra mutable visited storage | None beyond common cursors | `ceil(n/8)` disk bytes, bounded dirty-page cache |
| Extra immutable inverse payload | Positions as well as factor IDs | Factor IDs suffice |
| Candidate membership operation | Scan inverse until prefix witness | Bit lookup; set once on discovery |
| Potential repeated source work | `P`, sharply quadratic in incidence degrees | `O(Z)` candidate bit tests, plus root/ID handling |
| Access-type tradeoff | Immutable indexed source reads | Mutable random bitmap reads/writes |

With straightforward 64-bit fields, positioned inverse rows add about `8Z`
payload bytes over factor-only inverse rows, before indexes, alignment, and
encoding choices. Compare this with approximately `n/8` bitmap bytes, not
with an `n`-word visited array. Their payload ratio is about `64Z/n`, not a
measured total-storage ratio. Positions can sometimes be implicit or already
needed elsewhere; otherwise their construction and retention are an added
bill. Conversely a disk bitmap has initialization, address lookup, dirty-
page, and writeback costs. Neither cache locality nor overall winner follows
from asymptotic resident space alone. No physical-cache/full-copy comparison
or wall-clock bitmap benchmark was performed in this review.

## Independent Checker

This small model deliberately uses an ordinary explicit discovered map to
drive traversal and asserts equivalence with prefix tests at every candidate.
It also checks the whole prefix union after every consumption, finish
exhaustion, real parent edges, and the ancestor characterization on every
expanded edge. Its recursion and retained oracle state are *test machinery*,
not the bounded production executor. All ordered subsets of three labeled
vertices, zero through three factors, and every inverse-row permutation are
enumerated, including empty/repeated factors and isolates. No lead test
helpers are imported. The sharp family also checks feasibility of the
worst-case rank order, not just an abstract permutation inequality.

```python
from itertools import permutations, product


def check_prefix_forest_model(factors, inverse):
    n = len(inverse)
    cursor, parents, costs = [0] * len(factors), {}, [0] * n
    checks = 0

    def assert_prefix_union_exact():
        nonlocal checks
        union = {v for f, row in enumerate(factors) for v in row[:cursor[f]]}
        assert set(parents) == union
        checks += 1

    def consume_clique_candidate_exact(f, parent):
        v = factors[f][cursor[f]]
        old, hit = v in parents, False
        for g, p in inverse[v]:
            costs[v] += 1
            if p < cursor[g]:
                hit = True
                break
        assert hit == old
        cursor[f] += 1
        if not old:
            parents[v] = parent
        assert_prefix_union_exact()
        if not old:
            for g, _ in inverse[v]:
                while cursor[g] < len(factors[g]):
                    consume_clique_candidate_exact(g, v)
            assert all(cursor[g] == len(factors[g]) for g, _ in inverse[v])

    assert_prefix_union_exact()
    for f, row in enumerate(factors):
        while cursor[f] < len(row):
            consume_clique_candidate_exact(f, None)
    for v, row in enumerate(inverse):
        if not row:
            parents[v] = None
    assert len(parents) == n
    ancestors = []
    for v in range(n):
        chain, u = set(), v
        while u is not None:
            assert u not in chain
            chain.add(u)
            p = parents[u]
            assert p is None or any(u in row and p in row for row in factors)
            u = p
        ancestors.append(chain)
    for row in factors:
        for u, v in permutations(row, 2):
            assert u in ancestors[v] or v in ancestors[u]
    for v, row in enumerate(inverse):
        d = len(row)
        assert costs[v] <= (d * (d + 3) // 2 - 1 if d else 0)
    return costs, checks


rows = [r for k in range(4) for r in permutations(range(3), k)]
cases = snapshots = 0
for f in range(4):
    for factors in product(rows, repeat=f):
        inverse = [[(g, p) for g, row in enumerate(factors)
                    for p, v in enumerate(row) if v == u] for u in range(3)]
        for order in product(*(tuple(permutations(row)) for row in inverse)):
            _, checks = check_prefix_forest_model(factors, order)
            cases += 1
            snapshots += checks
print("ordered cases", cases, "prefix snapshots", snapshots, "counterexamples", 0)
for d in (3, 4, 8, 16, 32, 128):
    factors = ([(0,)] + [(0, i, i + 1) for i in range(1, d - 2)]
               + [(0, d - 2), (1, 0)])
    inverse = [[(g, p) for g, row in enumerate(factors)
                for p, v in enumerate(row) if v == u] for u in range(d - 1)]
    inverse[0].reverse()
    for v in range(1, d - 1):
        inverse[v].sort(key=lambda occurrence: occurrence[0] != v)
    costs, _ = check_prefix_forest_model(factors, inverse)
    assert costs[0] == d * (d + 3) // 2 - 1
    assert sum(costs) == sum(len(r) * (len(r) + 3) // 2 - 1 for r in inverse)
    print("sharp d/Z/P", d, sum(map(len, factors)), sum(costs))
```

## Execution Receipts and Counterexamples

Independent checker: **135,850 ordered cases; 1,202,344 whole-union invariant
snapshots; zero counterexamples.** The case counts by factor count `0,1,2,3`
are respectively `1,16,847,134986`. These are finite falsifiers, not a proof
by exhaustive testing for unbounded inputs. Run with assertions enabled.

Additional actual-code checks were run through an independent fixture and
oracle using `python3 -B`, without importing the lead's tests. Only ephemeral
temporary scratch files were created; no test/source file was added.

| Actual probe check | Receipt |
|---|---|
| Every ordered two-factor case on three vertices, every inverse permutation, `B=1` | 847 runs; valid/no-cross forests; exact full occurrence-read identity; scratch absent after each |
| Sharp family, `B=2` | Six runs; `P` equals the tighter bound exactly |
| All nonnegative length-14 push/pop histories, followed by draining, at `B=1,2,3,7` | 13,728 stack histories; 32,522 block transfers; oracle contents and every top replacement correct |
| Same stack histories | Actual file length equals spilled-frame payload; resident count at most `2B`; consecutive transfers separated by at least `B` operations; equal read/write bytes after draining; scratch removed |

Sharp-family results, independently measured from the actual executor:

| d | Z | P | All occurrence calls | Stack depth |
|---:|---:|---:|---:|---:|
| 3 | 5 | 12 | 27 | 2 |
| 4 | 8 | 21 | 45 | 3 |
| 8 | 20 | 67 | 127 | 7 |
| 16 | 44 | 207 | 339 | 15 |
| 32 | 92 | 679 | 955 | 31 |
| 128 | 380 | 8,887 | 10,027 | 127 |

**Observed failure boundaries:**

- With one factor and `block_frames=0`, `factor_size(0)` was called before
  `ValueError("block_frames must be a positive integer")`; no scratch file
  existed afterward. The constructor validates block size at lines 19-21,
  but the executor constructs it only at line 138, after source reads.
- On a generated 17-vertex clique, `B=2`, an `emit` callback that appended
  `(5,4)` and then raised left six externally visible records
  `[(0,None),(1,0),(2,1),(3,2),(4,3),(5,4)]`. Scratch held 32 bytes at the
  failure and was absent after unwinding. This exercises cleanup *after*
  spilling, unlike a failure on the first output call.
- Wrapping the opened file to perform a short first spill write produced
  `OSError("short DFS stack write")` after five records were emitted.
  Wrapping it to return a short first refill produced
  `OSError("short DFS stack read")` after all 17 records were emitted.
  Both paths removed scratch. Runtime wrappers were restored afterward.

The `with` block's close/unlink path covers normal Python exceptions,
including source/sink/I/O failures inside it. Exclusive `x+b` creation avoids
overwriting an existing pathname. It does not promise cleanup after process
death, failed unlink, hostile pathname replacement, or power failure.
The final vertex-count check occurs after output and cleanup; it is a
consistency guard, not provider validation or transactional publication.

There is no valid-input forest counterexample here. There *are* minimal
counterexamples to tempting modifications: advancing before testing makes
the sole candidate in factor `[v]` appear old; marking `v` as a root without
consuming any position makes `D={v}` but `U(p)={}`. Bulk-discovering the two
children of a root in one three-vertex clique yields a star whose child-child
edge is a cross edge. The current code avoids all three mistakes. The sharp
family above is the counterexample to a general `O(Z)` prefix-test claim.

Reviewed implementation SHA-256:
`c7b1c0b9abc150af576ca208c670d4115a9090e9c8f55879aef53b0aae135aae`.
Reviewed test SHA-256:
`54809ac8f0788b0679a40298ec142ec320d0eb65d9561eb3ca097ea5ecd8d432`.

## Closest Sources and Claim Boundary

**DCC is the starting point, not a distant analogy.** Ullah and Pothen,
[Succinct Graph Representations and Algorithmic Applications, Section 4.2,
Figure 7 and Lemma 4.8](https://arxiv.org/pdf/2604.28096#page=14), already use
global ordered clique cursors, incidence-dual iteration, immediate recursive
descent, per-vertex discovery indicators and parent pointers. They prove an
incidence-size time/space bound. Their Section 2 excludes isolates. Those
passages were inspected directly here, alongside the parent's full-text
follow-up. The candidate's delta relative to that displayed algorithm is the
prefix iff witness replacing discovery flags, consumed-root discipline,
positioned inverse requirement, streamed output and explicitly external
stack, with the newly quantified work penalty. Global cursors and native
clique DFS are inherited. No DCC performance measurement transfers here.

**Space-efficient DFS already has strong bounds.** Hagerup,
[Space-Efficient DFS and Applications: Simpler, Leaner, Faster](https://arxiv.org/abs/1805.11864),
Sections 2-4.1 and Theorem 4.1 in the inspected full text, uses adjacency-
position "turn" information to compress the DFS stack. Its input provides
constant-time `deg`, `head`, and reverse-position `mate` access, and that
algorithm retains `n` discovery bits. It is precedent for source-position
state encoding and explicit access assumptions, not proof of this exact
union-of-prefix discovery invariant. Bits versus words, expanded edges
versus native incidences, and resident versus external storage must remain
separate when comparing the bounds.

**Prepared encodings can absorb search information.** Elberfeld, Kammer and
Meintrup, [Space-Efficient Depth-First Search via Augmented Succinct Graph
Encodings, ISAAC 2025](https://drops.dagstuhl.de/storage/00lipics/lipics-vol359-isaac2025/html/LIPIcs.ISAAC.2025.29/LIPIcs.ISAAC.2025.29.html),
abstract and Section 1 inspected, exploit separable graph classes, prepared
nested divisions and augmentations; the encoding contains the DFS result and
supports later queries. Their sublinear post-preparation DFS bounds do not
mean streaming `n` original-ID parent records in sublinear time. The graph
class, relabeling, preprocessing and output contracts differ substantially.
Nevertheless, broad claims of first eliminating conventional vertex state
through prepared graph representations would be unjustified.

A bounded additional search for DFS/clique/prefix/occurrence/visited phrases
did not establish an exact earlier match. That is neither a priority proof
nor evidence of absence. The defensible result here is the conditional
correctness theorem, attainable work characterization and explicit access-
type tradeoff. Scientific priority, general packed-source construction,
physical end-to-end caps, fair bitmap timings and edited-WCC integration
remain open; this review does not mark the broader seven-family work complete.

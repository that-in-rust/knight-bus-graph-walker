# Communities: Independent Monotone-Frontier Review

Date: 2026-09-21. Bounded independent sidecar; this file is its only owned artifact.
No lead implementation is imported. No commit or push is part of this review.

## Premise Check

**Mathematical verdict: the proposed reduction is sound for the exact
matching extension, with qualifications on cursor semantics, final output, and
paid preparation.** The primitive is elementary monotone scanning, not evidence
of a new general priority queue or a universal low-RAM community solver. The
interesting candidate contribution is proving that this particular serial
community trajectory reduces to that primitive despite fragmented final labels.

Contract inspected: `Communities-Natural-Crossover.md`, lines 607-750, plus its
fixed graph and token contract. Here `b >= 16` is divisible by eight, `n=4b`, the
three weighted types have sizes `(2b,b,b)`, and Y has an arbitrary weight-one
perfect matching. Original IDs, singleton initialization, `gamma=1/2`, smallest
token among equal positive gains, staying on zero gain, and the final full
no-move sweep are all part of the claim. This is one family within the larger
seven-family research goal; it does not validate the other six.

Notation: Y IDs below are local ranks `0,...,b-1`; global IDs add `2b`. Label
`P=1`, label `Q=3b+1`. Write `A=D-h`, and use `B_io` for the external-memory
block size to avoid confusing it with the source document's score band `B`.
In this source's score normalization, `M'=sum_v degree(v)` and `D=2M'`;
do not substitute the common Louvain notation `m=sum_v degree(v)/2` for M'.

## Expert Lenses And Alternatives

- Serial-semantics lens: preserve strict gains, original order, wraps, and stops.
- Data-structure lens: prove exactly which deleted-set queries two cursors answer.
- External-memory lens: charge indexing, buffering, output spools, and validation.
- Adversarial lens: distinguish a special reduction from familiar scan machinery.

Compared alternatives: the source's delete-only union-find successor sets;
external time-forward messages/priority queues; an offline deletion log followed
by a join/sort to recover labels; and the proposed pair of frontier scans. The
last wins this restricted state machine because deletions are always minima and
the identity of the deleting side determines the final label. The other options
remain appropriate when these restrictions fail.

## Chosen Thesis: Exact Reduction

### 1. Three States, With Strict Inequalities

Use the source's coefficients, without changing the graph:

```text
h = 100b^2+20b+1                  D = 150b^2-64b
A = D-h = 50b^2-84b-1           B = D+h = 250b^2-44b+1
T = 150b^3+73b^2-10b            d(r) = T-2hr
r0 = 3b/4                      k = r0-1
d(r0) = 43b^2-(23/2)b
```

At band entry all low endpoints `<k` are Q and all high endpoints `>=k` are P.
Low-low matching pairs are fixed Q; high-high pairs are fixed P. Only crossing
pairs are unresolved. Their high (P) endpoint has gain `d+A`; their low (Q)
endpoint has gain `A-d`. The following table is exact:

| State | d relative to A | Eligible unresolved endpoints |
| --- | --- | --- |
| `r0-1` | `d>A` | high/P only |
| `r0` | `-A<d<A` | both |
| `r0+1` | `d<-A` | low/Q only |

For the tight inequality, `A-d(r0)=7b^2-(145/2)b-1`, which is 631 at
`b=16` and strictly increasing thereafter. Also `A+d(r0)>0`,
`d(r0-1)-A=193b^2+(225/2)b+3>0`, and
`d(r0+1)+A=-107b^2-(271/2)b-3<0`.
The same tight margin gives `B-d(r0-1)>0`; the other band edges have positive
margins too. No zero-gain tie occurs for an unresolved endpoint in these states.
No token tie can override ID visitation order: tokens break destination ties
*within a vertex's decision*, not ties between different vertices.

The source's band invariant freezes reunited pairs. In particular, an internal
pair must never be accidentally selected just because the generic endpoint
eligibility flag is true. It is filtered by pair status first.

### 2. HHLL Is A Serial-Order Statement

Let H delete the least still-unresolved high endpoint's pair, assigning both
endpoints Q. Let L delete the least still-unresolved low endpoint's pair,
assigning both P. The remaining accepted moves are the length-c prefix of
`H,H,L,L,H,H,L,L,...`.

Reason: after the forced prefix, the serial cursor is at `k-1` in sweep two and
only H is eligible. The first H reaches `r0`; the next eligible vertex after it
is still high, since every low ID is smaller than every high ID. The second H
reaches `r0+1`, forcing a wrap before any L can execute. The first L reaches
`r0`, and the next unresolved low precedes every unresolved high. The second L
reaches `r0-1`; a high phase then occurs later in that same sweep. Induction
uses only deletions: the least remaining endpoint in each class never decreases.

Since `k` is odd, the number `c` of crossing pairs is odd, and
`1 <= c <= b-k=b/4+1`. All full groups have length two; the last has length one.
Ending after an even number of completions is not possible for this family.
There are `(c+1)/2` H and `(c-1)/2` L moves, leaving `r=r0`.

With sweep one numbered 1, completion number `j` (zero based) has:

```text
side(j) = H if floor(j/2) is even, otherwise L
sweep(j) = 2 + floor((j+2)/4)
last moving sweep = 2 + floor((c+1)/4)
total sweeps including final no-move sweep = 3 + floor((c+1)/4)
total accepted moves = 4b-2+k+c
```

Every low phase wraps exactly once. A high phase following a low phase does not
wrap. If the last group is a single L, the remaining high suffix of that sweep
is still logically visited and stays; a separate full no-move sweep is required.
Disk cursors do not rewind at these logical wraps.

### 3. Correctly Scoped Frontier Lemma

Given a finite set of pairs, provide one record per pair in each of two fixed
total orders. Every record includes its partner's position in the opposite
order. A command chooses an order and deletes its least alive pair. Ties, if
keys repeat, must already have a fixed total-order tie-break.

Let `p_i` be the first *unconsumed* position in list i. All records before it
belong to deleted pairs. For a record at position `t >= p_i`, and only for such
an unread record,

```text
pair is already deleted <=> partner_position < p_other.
```

Proof by induction: no pair is initially deleted. Advancing over a stale record
consumes no alive pair. Selecting the first nonstale record deletes the minimum
alive pair and consumes its own record, putting its counterpart behind the
opposite-cursor test when that counterpart is eventually reached. Conversely,
an unread record cannot have caused its own pair's deletion, so a deleted pair
with this unread occurrence must have been consumed in the other list. This
also proves that every consumed prefix consists of deleted pairs. The test uses
strict `<`, not `<=`, for a first-unconsumed cursor.

For arbitrary positions, the correct deletion predicate is instead
`pos0 < p0 OR pos1 < p1`. The abbreviated opposite-cursor predicate is false on
some already-consumed records. For example, after selecting a lone pair in list
0, `p0=1,p1=0`, but its partner's position 0 is not behind `p1`.

Equivalently, the global invariant, valid for every pair and every policy that
selects a minimum remaining pair in one of these orders, is

```text
alive(pair) <=> pos0(pair) >= p0 AND pos1(pair) >= p1.
```

In rank-coordinate geometry, the survivors are precisely the input permutation
points in the upper-right orthant based at `(p0,p1)`. This is a useful equivalent
proof, not a claim to a new general geometric data structure. The constant-state
property comes from restricting all deletions to minima of fixed orders.

No tombstone set, bitmap, random mate lookup, or union-find is necessary during
this scan; no cancellation queue is needed either. Only the current record,
two cursors, counters, and a constant number
of I/O buffers are necessary. The command schedule need not be HHLL for this
generic lemma. The community reduction is what establishes that schedule.

### 4. ID Streams And Final Attribution

Store all Y endpoint records `(id,mate_id)` in original-ID order, with independently
buffered streams for `[0,k)` and `[k,b)`. Use first-unconsumed *ID boundaries* as
cursors, including fixed-pair records. Since ranks here are dense, the test is
simply `mate_id < opposite_boundary`. If the files instead contain only crossing
endpoints, store actual partner ranks or compare consistently with original-ID
frontiers; do not compare an ID against a filtered-list offset.
Block prefetch/read-ahead is not cursor advancement: only records logically
consumed under the proof may be put behind a frontier. Buffered, unconsumed
records can still be alive.

| Record being consumed | Why consumed | Final label |
| --- | --- | --- |
| high endpoint | selected H | Q |
| high endpoint | stale, selected earlier from low | P |
| low endpoint | selected L | P |
| low endpoint | stale, selected earlier from high | Q |
| either endpoint of low-low pair | fixed | Q |
| either endpoint of high-high pair | fixed | P |

Emit **only that record's own ID** when it is consumed; do not emit both endpoint
IDs at the instant a pair is selected. The opposite endpoint is emitted later
when its own stream reaches it. Each stream is therefore individually ID-sorted,
even when its labels alternate frequently.

After the cth selection, drain both streams to EOF. Every remaining crossing
record must be stale; encountering a live one is an assertion failure. Fixed
pairs still need output. Concatenate **low output then high output**, regardless
of which stream ran first. Together with known X/P and Z/Q blocks, this gives
the full original-ID output without a final sort. Two temporary output streams
and a later concatenation scan are an uncomplicated implementation; alternatively
one may use predetermined disjoint output segments in an appropriate file format.
Immediate, irrevocable, single-stream ID output during completion is not claimed.

### 5. Paid Resources And Input Boundary

- Trusted compact input: constant-size weighted template, ID ranges, b, and the
  arbitrary matching as explicit endpoint pairs. Expand each pair into its two
  directed mate records and externally sort by endpoint ID. A sequential scan
  can validate range, no self-pair, exactly one endpoint record for each Y ID,
  and count c. Symmetry follows from emitting both directions from each pair.
  If arbitrary mate records are the input instead, reciprocity must also be
  validated, for example by a sorted reverse-record comparison.
- Preparation upper bound: `O(Sort(b))` block transfers, `O(b)` external words.
  Conventional external merge sorting uses its own declared `M` memory budget;
  do not silently charge the build as a one-pass, constant-memory operation.
  A constant-fan-in sorter can use `O(B_io)` RAM with its corresponding larger
  merge depth. No comparison-sorting lower bound for all integer-ID methods is
  asserted.
- Prepared query/completion and full output: `O(Scan(n))` block transfers and
  `O(n)` word work, `O(B_io)` words of RAM for a constant number of buffers plus
  `O(1)` scalar words. `Scan(N)=Theta(1+N/B_io)`; the buffer count must fit the
  declared RAM budget. Output/spool space is `O(n)` external words, not O(1).
- Numerical model: coefficients/gains have `O(log n)` bits, possibly a constant
  multiple of the vertex-ID word width. A few exact multiword integers suffice;
  fixed-width implementations must check overflow. A Python verifier using big
  integers is not a machine-level RAM or disk benchmark.
- Explicit dense raw graph: it has `Theta(n^2)` edges/records. Exact validation
  or discovery of the supplied template is not made cheap by the query theorem.
  In particular an arbitrary uninspected edge could violate the representation.
  Reading such an input alone costs `Omega(Scan(n^2))` in this representation.
- Full decision-by-decision no-op logs have a larger output contract than final
  labels plus accepted moves and sweep markers. Do not claim scan-size output
  for explicitly printing every visited vertex in every logical sweep.

The strong same-input comparator is allowed the same theorem and preparation.
It can adopt the very same two-cursor scan. Thus this is a potentially useful
new *reduction/proof for the family*, not an asymptotic separation from every
specialized scalar simulator. Linear label fragmentation forbids neither scans
nor external output; it obstructs particular interval/event encodings.

## Independent Executable Mathematical Checker

The Python block below is self-contained and uses only the standard library.
It deliberately materializes test data, oracle label vectors, traces and sets.
These are verification scaffolding, not a claim to implement or measure the
external-memory algorithm. It imports no repository or lead code, writes no
files, and contains a separate expanded weighted-graph scalar oracle.

Run from the repository root, without extracting another file:

```sh
awk '/^```python$/{inside=1;next} inside && /^```$/{inside=0} inside{print}' research_algorithms_20260920/Communities-Monotone-Frontiers-Review.md | python3 -B -
```

```python
from itertools import permutations, product
from random import Random


def build_pair_mate_array(b, pairs):
    mate = [-1] * b
    for u, v in pairs:
        assert 0 <= u < b and 0 <= v < b and u != v
        assert mate[u] == mate[v] == -1
        mate[u], mate[v] = v, u
    assert all(v >= 0 for v in mate)
    return mate


def compute_exact_band_values(b):
    h = (10*b + 1)**2
    dscale = 150*b*b - 64*b
    t = 150*b**3 + 73*b*b - 10*b
    return h, dscale, t


def simulate_serial_y_decisions(mate, initial=None):
    b = len(mate)
    h, dscale, t = compute_exact_band_values(b)
    labels = [0]*b if initial is None else initial[:]
    r = sum(labels)
    trace = []
    sweep = 1
    while True:
        sweep += 1
        before = len(trace)
        for u in range(b):
            direction = 1 if labels[u] == 0 else -1
            mate_sign = 2*labels[mate[u]] - 1
            gain = direction*(t - 2*h*r + dscale*mate_sign) - h
            if gain > 0:
                labels[u] = 1 - labels[u]
                r += direction
                trace.append((sweep, u, r, gain))
        if len(trace) == before:
            return labels, trace, sweep
        assert sweep <= 2*b + 3


def simulate_two_stream_frontiers(mate):
    b = len(mate)
    k = 3*b//4 - 1
    h, dscale, t = compute_exact_band_values(b)
    c = sum(mate[u] >= k for u in range(k))
    assert c % 2 == 1
    cursor, end = [0, k], [k, b]
    output, reads = [[], []], [0, 0]
    trace, r = [], k
    for j in range(c):
        side = 1 if (j//2) % 2 == 0 else 0
        selected = False
        while cursor[side] < end[side]:
            u = cursor[side]
            v = mate[u]  # The current prebuilt endpoint record, not mate I/O.
            cursor[side] += 1
            reads[side] += 1
            crossing = (u < k) != (v < k)
            if not crossing or v < cursor[1-side]:
                output[side].append((u, 1-side))
                continue
            direction = 1 if side == 1 else -1
            gain = direction*(t - 2*h*r) + dscale - h
            assert gain > 0
            r += direction
            output[side].append((u, side))
            trace.append((2 + (j+2)//4, u, r, gain))
            selected = True
            break
        assert selected, (b, c, j, cursor)
    for side in (0, 1):
        while cursor[side] < end[side]:
            u = cursor[side]
            v = mate[u]
            cursor[side] += 1
            reads[side] += 1
            crossing = (u < k) != (v < k)
            assert not crossing or v < cursor[1-side]
            output[side].append((u, 1-side))
    rows = output[0] + output[1]
    assert [u for u, label in rows] == list(range(b))
    assert reads == [k, b-k]
    assert r == 3*b//4
    return [label for u, label in rows], trace, 3 + (c+1)//4


def verify_single_matching_instance(mate):
    b = len(mate)
    k = 3*b//4 - 1
    labels, trace, sweeps = simulate_serial_y_decisions(mate)
    out, completions, final_sweep = simulate_two_stream_frontiers(mate)
    assert labels == out and sweeps == final_sweep
    assert [(s, u, r) for s, u, r, g in trace[:k]] == [
        (2, u, u+1) for u in range(k)]
    assert trace[k:] == completions
    pairs = [tuple(sorted((u, mate[u]))) for s, u, r, g in completions]
    assert len(set(pairs)) == len(pairs)
    assert all(labels[u] == labels[mate[u]] for u in range(b))
    assert sum(labels) == 3*b//4
    h, dscale, t = compute_exact_band_values(b)
    assert all(abs(t-2*h*r) <= h+dscale for s, u, r, g in completions)
    return labels, trace, sweeps


def build_canonical_crossing_matching(c, perm):
    b = max(16, 8*((c+2)//2))
    k = 3*b//4 - 1
    pairs = [(i, k+perm[i]) for i in range(c)]
    pairs += [(i, i+1) for i in range(c, k, 2)]
    pairs += [(i, i+1) for i in range(k+c, b, 2)]
    return build_pair_mate_array(b, pairs)


def verify_generic_cursor_lemma():
    cases = 0
    for n in range(7):
        for perm in permutations(range(n)):
            lists = [tuple(range(n)), perm]
            ranks = [{v: i for i, v in enumerate(row)} for row in lists]
            for commands in product((0, 1), repeat=n):
                alive, cursor, read_counts = set(range(n)), [0, 0], [0, 0]
                for side in commands:
                    wanted = next(v for v in lists[side] if v in alive)
                    while True:
                        assert cursor[side] < n
                        v = lists[side][cursor[side]]
                        stale = ranks[1-side][v] < cursor[1-side]
                        assert stale == (v not in alive)
                        cursor[side] += 1
                        read_counts[side] += 1
                        if not stale:
                            assert v == wanted
                            alive.remove(v)
                            assert all((x in alive) == (
                                ranks[0][x] >= cursor[0] and
                                ranks[1][x] >= cursor[1]) for x in range(n))
                            break
                assert not alive
                for side in (0, 1):
                    while cursor[side] < n:
                        v = lists[side][cursor[side]]
                        assert ranks[1-side][v] < cursor[1-side]
                        cursor[side] += 1
                        read_counts[side] += 1
                assert read_counts == [n, n]
                cases += 1
    return cases


def simulate_expanded_graph_oracle(mate):
    b, n = len(mate), 4*len(mate)
    types = [0]*(2*b) + [1]*b + [2]*b
    template = ((11, 3, 0), (3, 0, 4), (0, 4, 11))
    weights = [[0]*n for _ in range(n)]
    for u in range(n):
        for v in range(u):
            w = template[types[u]][types[v]]
            if types[u] == types[v] == 1 and mate[u-2*b] == v-2*b:
                w += 1
            weights[u][v] = weights[v][u] = w
    degrees = [sum(row) for row in weights]
    total_degree = sum(degrees)
    assert total_degree == 75*b*b - 32*b
    labels, trace, sweep = list(range(n)), [], 0
    while True:
        sweep += 1
        before = len(trace)
        for u in range(n):
            # Recompute all candidate scores directly from weighted neighbors.
            scores = {token: 0 for token in labels}
            scores[n] = 0  # Fresh; tests assert it is never accepted.
            for v in range(n):
                if v != u:
                    scores[labels[v]] += (
                        2*total_degree*weights[u][v] - degrees[u]*degrees[v])
            source = labels[u]
            target = min(scores, key=lambda token: (-scores[token], token))
            gain = scores[target] - scores[source]
            if gain > 0:
                assert target != n
                labels[u] = target
                trace.append((sweep, u, target, gain))
        if sweep == 1:
            assert labels == [1]*(3*b) + [3*b+1]*b
            assert len(trace) == 4*b-2
        if len(trace) == before:
            return labels, trace, sweep
        assert sweep <= 2*b + 3


def verify_expanded_graph_case(mate):
    b = len(mate)
    bits, reduced, sweeps = verify_single_matching_instance(mate)
    labels, dense, dense_sweeps = simulate_expanded_graph_oracle(mate)
    assert labels == [1]*(2*b) + [3*b+1 if q else 1 for q in bits] + [3*b+1]*b
    tail = [row for row in dense if row[0] >= 2]
    assert len(tail) == len(reduced)
    for raw, small in zip(tail, reduced):
        rs, ru, target, rg = raw
        ss, su, r, sg = small
        assert (rs, ru, rg) == (ss, su+2*b, sg)
    assert dense_sweeps == sweeps


def verify_unsafe_generalization_examples():
    # '<=' would falsely delete a live singleton at two initial cursors of 0.
    assert not (0 < 0) and (0 <= 0)
    # Opposite-cursor test is not a predicate for already-consumed records.
    assert not (0 < 0) and (0 < 1 or 0 < 0)
    # Arbitrary deletion B in identical orders (A,B) cannot leave both
    # cursors at zero and still recognize B as deleted by that predicate.
    assert not (1 < 0)
    # b=8 violates the advertised middle-state eligibility and prefix length.
    h, ds, t = compute_exact_band_values(8)
    assert t-2*h*6 > ds-h
    assert t-2*h*5 > ds+h
    # Dispersed low IDs: at the middle state, a low endpoint can intervene.
    pairs = [(0, 1), (2, 3), (4, 5), (6, 7), (8, 9),
             (10, 11), (12, 13), (14, 15)]
    mate = build_pair_mate_array(16, pairs)
    lows = {0, 2, 4, 6, 7, 8, 9, 10, 11, 12, 13}
    initial = [int(u in lows) for u in range(16)]
    _, trace, _ = simulate_serial_y_decisions(mate, initial)
    assert [u for s, u, r, g in trace] == [1, 2, 5]
    assert ['L' if u in lows else 'H' for s, u, r, g in trace] == ['H', 'L', 'H']
    # A real c=1 instance still requires a drain; scanning stops after one H.
    mate = build_canonical_crossing_matching(1, (0,))
    b, k = len(mate), 3*len(mate)//4 - 1
    assert mate[k] == 0 and k > 0 and k+1 < b
    verify_single_matching_instance(mate)
    pairs = [(0, 11), (1, 12), (2, 13), (3, 4), (5, 6),
             (7, 8), (9, 10), (14, 15)]
    mate = build_pair_mate_array(16, pairs)
    labels, trace, _ = verify_single_matching_instance(mate)
    assert [u for s, u, r, g in trace[11:]] == [11, 12, 2]
    assert labels[13] == 0 and labels[0] == labels[1] == 1
    assert [11, mate[11]] != sorted([11, mate[11]])
    return 7


def run_independent_mathematical_checks():
    band_cases = 0
    for b in list(range(16, 4097, 8)) + [8*10**6, 8*10**18]:
        h, ds, t = compute_exact_band_values(b)
        a, band, r0 = ds-h, ds+h, 3*b//4
        states = [t-2*h*r for r in (r0-1, r0, r0+1)]
        assert states[0] > a and -a < states[1] < a and states[2] < -a
        assert all(-band <= d <= band for d in states)
        assert t-2*h*(r0-2) > band and t-2*h*(r0+2) < -band
        assert (t-band + 2*h-1)//(2*h) == r0-1
        band_cases += 1
    generic_cases = verify_generic_cursor_lemma()
    canonical_cases = 0
    for c in (1, 3, 5, 7):
        for perm in permutations(range(c)):
            verify_single_matching_instance(build_canonical_crossing_matching(c, perm))
            canonical_cases += 1
    rng = Random(20260921)
    random_cases, dense_inputs, max_runs = 0, [], 0
    for b in (16, 24, 32, 64, 128, 256):
        for trial in range(160):
            ids = list(range(b))
            rng.shuffle(ids)
            mate = build_pair_mate_array(b, zip(ids[::2], ids[1::2]))
            labels, trace, sweeps = verify_single_matching_instance(mate)
            max_runs = max(max_runs, 1+sum(a != z for a, z in zip(labels, labels[1:])))
            random_cases += 1
            if b in (16, 24, 32) and trial < 4:
                dense_inputs.append(mate)
    dense_inputs += [build_canonical_crossing_matching(c, tuple(range(c)))
                     for c in (1, 3, 5, 7)]
    for mate in dense_inputs:
        verify_expanded_graph_case(mate)
    negative_cases = verify_unsafe_generalization_examples()
    print('PASS exact band/prefix cases:', band_cases)
    print('PASS generic permutation/schedule cases:', generic_cases)
    print('PASS canonical crossing-order cases:', canonical_cases)
    print('PASS seeded arbitrary matching cases:', random_cases)
    print('PASS expanded singleton-start graph cases:', len(dense_inputs))
    print('PASS unsafe-generalization witnesses:', negative_cases)
    print('Observed maximum Y label runs in random cases:', max_runs)
    print('All comparisons include completion IDs, exact gains, r, sweeps, labels, and drains.')


run_independent_mathematical_checks()
```

## Evidence And Verification

### Executed Receipt

Run on 2026-09-21 using the command immediately above the Python block; exit 0:

```text
PASS exact band/prefix cases: 513
PASS generic permutation/schedule cases: 50363
PASS canonical crossing-order cases: 5167
PASS seeded arbitrary matching cases: 960
PASS expanded singleton-start graph cases: 16
PASS unsafe-generalization witnesses: 7
Observed maximum Y label runs in random cases: 84
All comparisons include completion IDs, exact gains, r, sweeps, labels, and drains.
```

The generic test fixes one order to identity, enumerates every permutation of
the other for sizes 0 through 6, and every length-n binary command schedule.
This loses no relative-order case at those sizes. It checks the global alive
invariant after each deletion as well as the unread-record test during scans.
The community-specific exhaustive part covers every relative crossing order
for c in `{1,3,5,7}`. Fixed internal pairs and ID gaps cannot change these
relative-order decisions under the proved filtering rule; random perfect
matchings separately exercise varied endpoint locations for b in
`{16,24,32,64,128,256}`. This is not an exhaustive test of every perfect matching
at those b values. The algebra, not these finite experiments, supplies the
all-b claim.

The expanded oracle recomputes vertex degrees and every active community's
score directly from a weighted adjacency matrix, including fresh and source
choices. It verifies the actual singleton first sweep and all later accepted
IDs/gains and final labels/sweeps. It does not call either frontier logic or
the reduced coefficient function for its decisions. The late-Y reduced oracle
independently scans all Y vertices on every sweep, starting with all Y in P.
Neither oracle is the lead implementation. These checks are mathematical
cross-checks, not a physical external-sort, buffer, I/O, or RAM benchmark.

### Counterexamples And Unsafe Generalizations

1. **`<=` instead of `<`.** One pair has ranks `(0,0)` and initial cursors
   `(0,0)`. The nonstrict test says it was deleted before any command. Under the
   first-unconsumed convention, only strict `<` is correct.
2. **Opposite-only predicate on all records.** One selection from order 0 leaves
   cursors `(1,0)`; that consumed record is dead although its opposite rank 0 is
   not below 0. Use the global OR deletion/AND survival invariant instead.
3. **Arbitrary deletion or advancing over an ineligible live record.** Both
   lists are `[a,b]`. Deleting b while a is alive cannot be represented by two
   prefix frontiers: any frontier that certifies b dead also certifies a dead.
   Similarly, skipping live a to find an eligible b invalidates the proof.
4. **Interleaved low/high IDs.** At b=16 take matching
   `(0,1),(2,3),(4,5),(6,7),(8,9),(10,11),(12,13),(14,15)`, and put
   `{0,2,4,6,7,8,9,10,11,12,13}` in Q. This has `r=r0-1=11` with three split
   pairs, but serial scanning selects IDs `1,2,5`, hence H,L,H, not H,H,L.
   It is a deliberate warm-state counterexample to dropping ID separation,
   not a counterexample reachable from this family's forced-prefix start.
5. **Extending to b=8.** `d(r0)=2660>A=2527`; also
   `d(r0-1)=15782>B=15649`. Thus the asserted middle state and k formula both
   fail. The bound `b>=16` is material, not cosmetic.
6. **Omitting the final drain.** At b=16, pair `(0,11)` crosses k=11; fill both
   remaining sides with adjacent internal pairs. The sole selection is H at
   11. All 11 low records and high records 12 through 15 still need output.
7. **Labels inferred from initial side.** With b=16 crossing pairs
   `(0,11),(1,12),(2,13)` and internal pairs
   `(3,4),(5,6),(7,8),(9,10),(14,15)`, the completions are H11,H12,L2.
   High 13 must eventually emit P, while low 0 and low 1 emit Q. Assigning all
   stale high records Q, or all stale low records P, reverses their labels.
8. **Emit both mates at selection time, then merely concatenate.** In that same
   example, the high stream's first selected ID is 11 but its mate ID is 0.
   Writing both into the high stream destroys its sorted-ID property. Stream
   membership follows the record's own original ID, not the selection event.
9. **A general community solver or arbitrary output labels.** The membership
   lemma is independent of communities, but the attribution theorem uses two
   possible selecting sides with fixed labels. Extra defect edges, changing
   priorities, reinsertion, different resolution/weights, or different tie rules
   require new trajectory and output proofs. No lower bound or impossibility
   for those richer problems is inferred from this restriction.

### Closest Primary Art

Bounded search performed 2026-09-21 across ordered elimination, multidimensional
priority queues, threshold scanning, external-memory clustering, and compressed
graph representations. The distinctions below are claims about the inspected
material, not a global novelty-absence result. No inaccessible full text is
treated as if it had been read.

**A. Same abstract operation: multidimensional priority queues.** Ding and Weiss,
*The K-D Heap: An Efficient Multi-dimensional Priority Queue*, WADS 1993,
pp. 302-313. Inspected the publisher's two-page preview: Section 1, pp. 302-303,
and the opening of Section 2. It explicitly discusses objects with several
priority relations and maintaining linked occurrences when separate queues are
used. The abstract describes minimum access in each dimension with logarithmic
updates. This is a direct ancestor of the abstract "choose one order's minimum,
remove that object everywhere" problem, not just a vaguely related heap.
The two fixed presorted lists and deletion-only contract here remove the dynamic
operations; neither a general k-d heap nor its RAM bound proves the scan kernel.
[Primary preview](https://page-one.springer.com/pdf/preview/10.1007/3-540-57155-8_257),
[publisher record](https://link.springer.com/chapter/10.1007/3-540-57155-8_257).

Brass, *Multidimensional heaps and complementary range searching*, IPL 102(4),
152-155 (2007), is an even more directly titled follow-up. The publisher's
displayed abstract defines delete-min under each coordinate. Full article text
was not accessible in this bounded inspection, so no internal theorem is
attributed to it. It reinforces the need to frame our primitive as a restricted
case of an established operation, not a new abstract data type.
[Publisher abstract](https://www.sciencedirect.com/author/7003276690/peter-brass),
[article DOI](https://doi.org/10.1016/j.ipl.2006.12.008).

**B. Monotone frontiers over several sorted views.** Fagin, Lotem and Naor,
*Optimal Aggregation Algorithms for Middleware*, PODS 2001 / full arXiv version
2002. Inspected Section 4, steps 1-3 and Theorem 4.1, printed pp. 6-7 of the
arXiv version. Their threshold algorithm uses the current position/value in each
sorted list to bound unseen objects, and performs random access for associated
attributes. This is established frontier-based certification, but the target is
top-k aggregation, not deletion history, HHLL serial sweeps, or final community
labels. Its constant-buffer statement must not be transferred to an unrelated
access model without proof. Here the opposite rank is embedded in the current
record and no such random access is needed.
[Primary full text, Section 4](https://arxiv.org/pdf/cs/0204046#page=7).

**C. Scanning and external graph scheduling.** Chiang, Goodrich, Grove, Tamassia,
Vengroff and Vitter, *External-Memory Graph Algorithms* (1995). Inspected the
author-hosted abstract and Section 1.1, printed p. 2, which defines scan/sort
costs. The abstract and results overview describe time-forward circuit-style
processing and its use for list ranking. Detailed later sections were not
reliably retrievable, so this review does not assign them a theorem number.
The scan cost is standard prior machinery. The family-specific achievement is
showing that changing communities can be represented by two monotone frontiers,
without a general message/cancellation queue.
[Primary author copy](https://www.ittc.ku.edu/~jsv/Papers/CGG95.external_graph.pdf).

**D. Actual external-memory clustering and sorted label recovery.** Akhremtsev,
Sanders and Schulz, *(Semi-)External Algorithms for Graph Partitioning and
Clustering* (2014). Inspected Section 4.1, printed pp. 4-5: external label
propagation uses current/next-round priority queues, forwarding according to
relative vertex IDs, with Sort(E) work per iteration; the semi-external version
keeps vertex labels/sizes resident. Section 5, printed pp. 7-8, explicitly sorts
vertex/cluster pairs and sorts recovered labels back to vertex order during
solution transfer. Section 7 identifies modularity optimization as further
work, so its update rule is not our exact reference. This is the closest
inspected external-memory community workflow. Our special two-ID-range output
avoids the generic recovery sort because its two outputs are already sorted
and range-separated; it does not improve their general clustering problem.
[Primary full text, Sections 4.1 and 5](https://arxiv.org/pdf/1404.4887#page=5).

**E. Template plus explicit exceptions is established representation art.**
Navlakha, Rastogi and Shrivastava, *Graph Summarization with Bounded Error*
(SIGMOD 2008). Inspected the author's thesis reproduction, Chapter 1,
Section 1.1, printed pp. 8-11: summary supernodes/superedges, positive/negative
edge corrections, and the original-node mapping reconstruct a graph. The
paper-host PDF timed out; the thesis is the accessible primary copy inspected.
It does not by itself prove that summary vertices can be contracted while
preserving a particular serial modularity trajectory. The present matching is
a linear-size exception list, not a constant-size type description. Our use of
a weighted template plus defects is structurally related to this representation;
pair freezing and serial-order elimination are the additional proof obligations.
[Primary thesis, Chapter 1](https://api.drum.lib.umd.edu/server/api/core/bitstreams/f6990b93-fd18-482b-a2e6-a11da1fe34cf/content#page=21).

**F. Very close compressed-score community art.** Yu, Srinivasan and Thomo,
*Efficient Vector-Based Louvain Algorithm for Massive Low-Rank Graphs*, EDBT
2026, pp. 537-543. Inspected Section 4 (degree calculation), Sections 4.1-4.2
(community affinity/degree aggregates), Algorithm 1, and Section 5.4 (costs).
The method avoids explicit dense edges using an inner-product representation,
and describes O(nd) resident feature/community/assignment storage. Algorithm 1
also takes a positive sweep tolerance. This is strong prior art against claiming
that exact algebraic community scores without explicit edges are new. Those
sections do not establish this review's O(B_io)-RAM, scan-I/O elimination theorem,
nor its strict no-move stopping contract. Do not use the paper's generic
implementation as a deliberately weak same-input comparator.
[Primary proceedings paper](https://openproceedings.org/2026/conf/edbt/paper-72.pdf).

**Independent representation distinction, not a claim quoted from VLouvain.**
One cannot absorb arbitrary matching defects into constant-dimensional exact
inner products for free. Restricted to Y and ordered by matching pairs, any
Gram matrix realizing these off-diagonal weights must be block diagonal with
b/2 blocks `[[a_i,1],[1,z_i]]`; diagonal values may be arbitrary. Each block has
rank at least one, so the feature dimension is at least b/2. This argument
allows the self-loop/diagonal adjustment and therefore avoids the unsafe claim
that a zero-diagonal adjacency's full rank alone rules out a small Gram model.
A separate sparse matching correction, as here, is a genuinely different input
representation. This does not rule out other low-rank-plus-sparse algorithms.

## Final Synthesis

The exact contract supports the proposed O(Scan(n)) prepared kernel and full
sorted output with O(B_io) buffers and O(1) scalar state. The HHLL pattern, the
wrapped logical sweeps, and the delayed counterpart label attribution survive
independent checking. The corrected global frontier invariant also works for
arbitrary policies choosing a minimum under either fixed order; HHLL is needed
for reproducing this community reference, not for the data-structure lemma.

The defensible manuscript contribution is a special exact serial-trajectory
reduction on a compact weighted-template-plus-matching family, including
fragmented outputs. It is not the invention of scanning, multi-order deletion,
template correction graphs, or community aggregate scores. It is not evidence
that all seven graph families have acquired genuinely new low-RAM algorithms.
The stronger scalar baseline can reuse the same reduction.

Suggested narrow statement:

> For the specified singleton-start matching family, after charged construction
> of original-ID mate streams, the exact accepted-move trajectory, sweep count,
> and original-ID final labeling can be generated with scan I/O and a constant
> number of block buffers. The result follows from pair freezing, separated
> endpoint orders, and a two-frontier invariant; it neither assumes short final
> label runs nor makes dense raw-input validation free.

## Open Questions

- Does an earlier deletion-only multidimensional-priority treatment state this
  exact presorted cursor lemma? The accessible k-d heap preview establishes the
  older abstract operation, not the historical status of this restricted lemma.
- Can more than this engineered graph family be certified to have fixed-order,
  deletion-only interactions without an O(n)-RAM discovery/verification step?
  Any broader contribution should identify a substantive new theorem here.
- Is the end-to-end storage build reusable enough to matter on real workloads?
  The O(Sort(n)) build and O(n)-word mate/output files remain part of the cost.
- Physical I/O, overflow limits, and actual buffer counts belong to the lead's
  separate implementation/probe. The lead reports successful independent tests;
  those results are not counted as this review's executed evidence.

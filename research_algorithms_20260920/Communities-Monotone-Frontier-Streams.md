# Exact Community Trajectories With Two Monotone Streams

Date: 2026-09-21. A04 follow-on. Status: proved within the declared family,
implemented research kernel, finite equivalence checks passed. Completed
independent mathematical review and retained scaling receipts appear below.
This is not a general community-detection algorithm or a publication claim.

## Result In One Paragraph

The matching-defect family in [the natural-crossover study](Communities-Natural-Crossover.md)
can force linearly many final ID-order label runs. Its strongest previously
derived scalar control retained delete-only endpoint sets. Those sets are
unnecessary for this family: unresolved pairs can be represented by **two
monotone cursor positions in an immutable mate file**. The exact serial
accepted-move trace, sweep numbers, final no-move sweep, and every original-ID
label can be emitted with constant control state and bounded sequential
buffers. After paid preparation, execution takes O(n) word operations and
O(Scan(n)) block transfers, including full labels and the accepted-move log.
The input matching and complete output still occupy Theta(n) disk words.

The important distinction is between a complicated answer and a complicated
resident state. Linear label fragmentation does not force linear RAM when
the dependency resolution can be expressed as monotone frontiers. This is
a new derived algorithm in this research portfolio; priority against the
literature remains a separate question.

## Research Plan And Acceptance Contract

This follows the existing approved seven-family research goal, rather than
starting an unrelated product feature. The alternatives considered were:

1. Keep the previous delete-only union-find structures: near-linear work but
   O(n) resident words in the stated implementation.
2. Spill cancellation records into external priority queues: bounded RAM,
   but introduces ordering and storage work that may not be necessary.
3. Prove a two-frontier invariant and use forward streams: selected here.
   It has stricter eligibility conditions, which must be proved rather than
   assumed for arbitrary community graphs.

Verification-first steps:

- Write independent expanded-graph and reduced scalar oracles; observe the
  candidate module missing before implementation.
- Compare every accepted original-ID move, not just final modularity or
  community cardinality. Include exact sweep numbers and final labels.
- Exercise all permutations of seven crossing pairs and seeded matchings.
- Implement the file path with fixed buffers and no retained label vector.
- Charge input preparation, two label spools, final output, and move log.
- Obtain a separate mathematical/prior-art challenge; preserve restrictions.

## Exact Input And Semantic Contract

Use precisely the earlier weighted three-type family: X has 2b vertices,
Y has b, Z has b; b>=16 is divisible by eight. Internal X/Z edge weight is
11, X-Y weight is 3, Y-Z weight is 4, X-Z and ordinary Y-Y weight are zero.
Add an arbitrary perfect matching of unit-weight edges on Y. There are no
self loops. Resolution is gamma=1/2. Original IDs put X, Y, Z consecutively.

Initialize every vertex in its own community, visit increasing original
IDs each sweep, choose the smallest community token on equal positive
gains, stay on zero gain, and stop only after an entire no-move sweep.
There is no contraction phase or change of visit order. This is the
specified local-moving routine, not the entire Louvain/Leiden pipeline.

The previous proof gives permanent X community P with token 1 and Z
community Q with token 3b+1. Sweep one has exactly 4b-2 accepted moves and
puts all Y in P. In sweep two, the first

```text
k = 3b/4 - 1
```

Y vertices move to Q, regardless of matching. From that point onward, a
split matching pair reunites on its next accepted move and never moves
again. Write c for the number of matching edges crossing [0,k) | [k,b).
Exactly c completions remain. Both k and c are odd; 1<=c<=b-k.

The kernel receives a trusted, previously validated reciprocal perfect
matching in original-ID order plus b and c. The implementation checks
scalar domains, record counts, mate range, self-pairs and crossing counts,
but does not prove global reciprocity with those local checks. It is not
a decoder for adversarial unvalidated graph files.

## Lemma 1: Three Score States Determine The Phase Schedule

Let r be the number of Y vertices in Q, r0=3b/4, and

```text
h = (10b+1)^2
D = 150b^2 - 64b
a = D-h = 50b^2 - 84b - 1
d(r) = 150b^3 + 73b^2 - 10b - 2hr
d(r0) = 43b^2 - (23/2)b
```

For a split pair, its P endpoint is eligible iff d>-a and its Q endpoint
iff d<a. For all supported b, -a<d(r0)<a. Also d(r0-1)>a and d(r0+1)<-a.
Thus the three reachable states after the prefix have eligibility:

| State | Eligible unresolved endpoints |
| --- | --- |
| r0-1 | P only |
| r0 | P and Q |
| r0+1 | Q only |

Every split Q endpoint is in the low-ID prefix; every split P endpoint is
in the high-ID suffix. Starting at r0-1 in sweep two, the next two accepted
moves therefore come from the smallest remaining high endpoints, unless
only one pair remains. At r0+1 only a low endpoint can move, and all low
IDs are behind the visit cursor, so the scalar routine wraps. It accepts
the two smallest remaining low endpoints, unless this is the last single
completion. The following high phase occurs in that same sweep.

Consequently the completion directions are:

```text
high, high | low, low | high, high | low, low | ... | one
   sweep 2    sweep 3     sweep 3    sweep 4
```

The last phase has one move because c is odd. Final r is r0. Including the
terminal no-move sweep, the number of logical sweeps is

```text
3 + floor((c+1)/4).
```

This is a schedule theorem, not permission to reorder decisions. Each phase
must select the actual smallest still-unresolved endpoint in that order.

## Lemma 2: Deletion History Is Two Prefixes

Consider any set of paired records with two total orders. An operation may
choose either order, but must remove its least still-live record. Store each
record's partner position in the other order. Let L and H be the next unread
positions of the two streams. Then the live set is exactly

```text
{pair e: position_low(e) >= L and position_high(e) >= H}.
```

Initially both prefixes are empty. When advancing one stream, a record
whose partner lies behind the other cursor was already removed there.
Skipping it changes no live pair. Otherwise the record is the least live
record in the chosen order; selecting it extends that order's consumed
prefix and deletes exactly that pair. Induction proves the invariant.

The two immutable orders may be an arbitrary permutation of each other.
No locality, short matching edges, small label-run count, bitmap, set,
union-find parent array, or cancellation queue is used in this proof.

For this graph, original mate IDs themselves are the other-order positions.
There is no need to allocate a rank-translation table: the two orders are
disjoint contiguous ranges of the original Y ID space. Same-side matching
pairs are skipped as fixed labels without affecting the crossing-pair lemma.

## Lemma 3: Sorted Output Needs No Result-Sized State

When a high endpoint is selected, both members of its pair finish in Q.
When a low endpoint is selected, both finish in P. What happens when a
stream later encounters an already removed pair?

- A skipped low endpoint has a high mate behind H, so that high endpoint
  selected the pair earlier. Emit Q.
- A skipped high endpoint has a low mate behind L, so that low endpoint
  selected the pair earlier. Emit P.

There is no ambiguity from other skipped records: if the opposite stream
had merely skipped this same pair, the current record would already be
behind its own cursor, contradicting that it is being read now.

Low-low pairs are permanently Q; high-high pairs are permanently P. Thus
each Y label is known on its only read. Emit low labels and high labels to
two sequential spools. The complete original-ID result is the concatenation

```text
2b copies of P | low-label spool | high-label spool | b copies of Q.
```

Linear output fragmentation is permitted. There is no final sort, arbitrary
label lookup, result-sized hash table, or assertion that output fits in RAM.

## Algorithm And Storage Layout

```text
Immutable prepared mate file: one u64 mate per Y ID

  [ low prefix: 0 .. k-1 ][ high suffix: k .. b-1 ]
             |                        |
       forward cursor L         forward cursor H
             |                        |
       compare mate >= H        compare mate >= L
             |                        |
       low labels spool         high labels spool
             +------------+-----------+
                          |
                 concatenate with X/Z
                          |
                 original-ID labels

Accepted moves from both cursors --> one chronological move log
```

After emitting the proved first-sweep and forced-prefix accepted moves,
alternate two high completions and two low completions, stopping after c.
Advance over fixed same-side pairs and already completed crossing pairs.
On each accepted completion, emit its actual original ID, destination token
and sweep. At termination, drain both streams for remaining fixed labels;
any supposedly live crossing pair during this drain contradicts the header.

The packed wrapper opens two read handles on the same immutable mate file,
seeks the high handle to 8k once, and never performs a per-pair random read.
It writes two label spools and a chronological accepted-move file. It then
assembles the final label file and removes only its own spools.

The full trace of *all visits*, including every stay in every sweep, can be
Theta(n*c) records. We do not pretend to emit that in linear time. The
contract is the complete accepted-move trace plus an explicit logical final
no-move sweep; the fixed scan order determines omitted stays. The previous
strong endpoint baseline used the same convention.

## Resource Theorem And Exact Packed Accounting

Put n=4b and let B be a block's word capacity. A block-buffered reader and
writer need O(B) words, plus O(1) control words of O(log n) bits. Every mate
record is read once; every accepted move and original label is emitted
once. Thus prepared execution costs O(n) word operations and O(Scan(n))
block transfers. There are only constantly many streams.

Because the contract materializes n fixed-width final labels, every method
already owes Omega(Scan(n)) output transfers. The prepared execution is
therefore output-optimal in this block model. This elementary output lower
bound does not make preparation free or establish an end-to-end speed ratio.

For the implemented u64 mate/label records and three-u64 accepted moves,
write m=4b-2+k+c. Byte accounting is:

| Quantity | Bytes |
| --- | ---: |
| Prepared mate input | 8b |
| Both label spools combined | 8b |
| Complete final labels, all 4b vertices | 32b |
| Complete accepted-move log | 24m |
| Source-to-output kernel logical reads plus writes | 56b+24m |
| Peak owned input, spools and outputs during assembly | 48b+24m |
| Retained outputs after spool cleanup | 32b+24m |

The peak formula excludes preexisting original source, preparation scratch,
other snapshots, filesystem metadata/rounding and runtime memory. It is an
owned logical-file formula, not total machine storage. Python buffered I/O,
interpreter state and OS cache make whole-process/host memory a separate
measurement. This prototype does not enforce a 4 GB or 5 GB physical cap.

The eight-byte label format uses the row position as its dense original ID.
If the consumer instead requires explicit `(u64 ID,u64 label)` rows, final
output becomes 64b bytes, adding 32b to each affected total above. Both
comparators must use the same output contract. Arbitrary external-ID maps
are additional paid data; this experiment does not silently include them.

### Preparation Is Not Free

If the input is an unsorted matching edge list, emit both endpoint/mate
records, externally sort by endpoint, validate one valid record per vertex,
and count crossing pairs. For an original edge list of non-self pairs, that
construction supplies reciprocity; arbitrary supplied mate arrays require
their own reciprocal validation. An ordinary external sort costs Sort(b)
I/Os, bounded chosen sort memory and Theta(b) scratch words. The kernel
does not implement this general builder.

If the *dense expanded graph* is supplied rather than a certified template
plus matching, ingesting and checking it costs Theta(n^2) edge inspection
in this family. The source representation is part of the admission
contract, not something discovered at zero cost. Detecting useful nearby
structure in customer data remains an open product/research task.

### Comparator Update

| Prepared-input method | Control state | Main execution work |
| --- | --- | --- |
| Repeated full scalar sweeps | Theta(n) labels | Can scan Theta(n*c) visits |
| Previous ordered-set control | Theta(n) words | O(n+c log c) |
| Previous delete-only successor control | Theta(n) words | O(n+c alpha(c)) |
| Two forward streams | O(1) words plus buffers | O(n), O(Scan(n)) I/O |

The first row is not the strongest baseline. The real advance relative to
our previous analysis is removing the delete-only structures while retaining
exact semantics. A comparator allowed to use this new reduction can of
course match it; that alone neither establishes nor refutes scientific
priority. Generic scanning, sorted concatenation and prefix membership are
not claimed as newly invented primitives.

## Rubber-Duck Corrections And Failure Boundaries

1. **A large final run count does not prove a RAM lower bound.** It proves
   an obstruction for that interval representation. We emit fragmented
   labels rather than retain their interval decomposition.
2. **A stream of arbitrary deletions is not enough.** Deleting a middle
   pair before both orders' first live pair cannot generally be represented
   as consumed prefixes. Least-live selection is essential.
3. **One exceptional neighbor is not by itself sufficient.** The theorem
   also needs the proved score band and contiguous low/high endpoint classes.
   Arbitrary matching weights or interleaved visit orders are not admitted.
4. **No pair reactivation is allowed.** General community moves can split
   a previously completed interaction. This family's invariant forbids it.
5. **The prepared matching remains linear on disk.** Constant scheduling
   state is not a constant-size encoding of an arbitrary matching.
6. **All visits and all accepted moves are different output contracts.**
   Full stay-event output can itself dominate the time bound.
7. **The independent expanded oracle must include gamma=1/2 correctly.**
   Its integer score is 2*sum(degrees)*weight-degree_product, not the
   gamma=1 score. That factor was corrected before the candidate was run.
8. **Trusted prepared input is not validation.** Local stream checks do
   not justify accepting arbitrary nonreciprocal mate files as graphs.
9. **Read-ahead is not a logical frontier.** A buffer may contain future
   records. The proof compares against consumed-record cursors, never the
   underlying file handle's buffered byte offset.
10. **The opposite-cursor test is local.** It applies to the currently
    unread record. For an arbitrary pair, use the global two-coordinate
    survival predicate, not just one opposite-rank comparison.

## Finite Verification Receipt

The initial test run had five expected missing-module failures. After the
implementation, all five tests passed (0.138 seconds). An explicit
equal-count/different-identity regression was then added; the final six-test
run passed in 0.139 seconds:

- 10 complete expanded-graph scalar trajectories: eight b=16 matchings,
  one b=24 and one b=32. Exact labels, every accepted original-ID move and
  sweep, and final no-move sweep agree.
- All 5,040 permutations of seven crossing pairs at b=24, with the same
  complete post-prefix trace comparison against an independent scalar
  score simulator. This is exhaustive for that subfamily, not all b=24
  perfect matchings.
- 150 seeded complete matching instances over b=16,24,32,64,128,256.
- A packed-file execution with 16-byte buffers, compared against the
  tested stream path for every output label and accepted move.
- Invalid headers, short/trailing streams and invalid mate IDs refuse.
  Single-pass wrappers reject restarting either input iterator.
- Identity and reverse crossing permutations have identical scalar receipt
  counts but different complete labels and traces. Keeping only c and final
  community sizes is not sufficient to reproduce the answer.

The collecting test callbacks deliberately retain outputs for comparison;
their memory is verifier memory, not the file executor's workspace.

Reproduce:

```sh
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover \
  -s research_algorithms_20260920/experiments \
  -p test_matching_frontier_communities.py -v
```

## Retained Packed Scaling Probe

The following probe generates a native certified template-plus-matching
input in forward order, executes the file path, checks output cardinality,
every matching pair's final agreement, and accepted-trace ID order. It is
not a general external-sort builder, dense-graph import or independent
large scalar simulation. Each b is run in a fresh process. Timings exclude
generation and the subsequent audit. Process peak RSS includes them.

<!-- FRONTIER-SCALING-PROBE-START -->
```python
from pathlib import Path
import hashlib
import json
import os
import resource
import struct
import sys
import tempfile
import time
sys.path.insert(0, 'research_algorithms_20260920/experiments')
from probe_matching_frontier_communities import run_matching_frontier_files

b = int(sys.argv[1])
k, c = 3*b//4-1, b//4+1
inverse_two = (c+1)//2
def matching_partner_at_rank(i):
    if i < c:
        return k+(2*i+1)%c
    if i < k:
        return c+((i-c)^1)
    return ((i-k-1)*inverse_two)%c

with tempfile.TemporaryDirectory() as temporary:
    root = Path(temporary)
    source = root/'mates.bin'
    with source.open('wb') as output:
        for i in range(b):
            output.write(struct.pack('<Q', matching_partner_at_rank(i)))
    started = time.perf_counter()
    receipt = run_matching_frontier_files(source, root/'output', b, c)
    receipt['kernel_seconds'] = time.perf_counter()-started
    receipt['b'] = b
    digest, q_count, transitions, last = hashlib.sha256(), 0, 0, None
    with (root/'output'/'labels.bin').open('rb') as labels:
        for original_id in range(4*b):
            packed = labels.read(8)
            digest.update(packed)
            label = struct.unpack('<Q', packed)[0]
            if original_id < 2*b:
                assert label == 1
            elif original_id >= 3*b:
                assert label == 3*b+1
            else:
                i = original_id-2*b
                assert label in (1, 3*b+1)
                q_count += label == 3*b+1
                transitions += last is not None and last != label
                last = label
                other = os.pread(labels.fileno(), 8, 8*(2*b+matching_partner_at_rank(i)))
                assert packed == other
        assert not labels.read(1)
    assert q_count == 3*b//4
    move_count, previous, move_digest = 0, (0, -1), hashlib.sha256()
    with (root/'output'/'moves.bin').open('rb') as moves:
        while packed := moves.read(24):
            move_digest.update(packed)
            sweep, vertex, label = struct.unpack('<QQQ', packed)
            assert (sweep, vertex) > previous
            assert vertex < 4*b and label in (1, 3*b+1)
            previous = (sweep, vertex)
            move_count += 1
    assert move_count == receipt['accepted_moves'] == 5*b-2
    assert receipt['sweeps'] == 3+(c+1)//4
    receipt.update(labels_sha256=digest.hexdigest(), moves_sha256=move_digest.hexdigest(),
                   y_transitions=transitions, process_peak_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                   rss_units='bytes' if sys.platform == 'darwin' else 'KiB', platform=sys.platform)
    print(json.dumps(receipt, sort_keys=True))
```
<!-- FRONTIER-SCALING-PROBE-END -->

Run from the repository root, substituting one b value per fresh invocation:

```sh
awk '/^<!-- FRONTIER-SCALING-PROBE-START -->/{p=1;next} /^<!-- FRONTIER-SCALING-PROBE-END -->/{p=0} p && !/^```/' \
  research_algorithms_20260920/Communities-Monotone-Frontier-Streams.md | \
  /Users/amuldotexe/.local/bin/python3.11 -B - 4096
```

### Observed Scaling Receipt

Each row is one fresh Python 3.11 process on this macOS host, with 65,536-byte
buffers. These are single observations without a comparison engine or timing
repetitions. They are not tail-latency estimates or physical-budget proofs.

| b | Implicit vertices n | Prepared mate bytes | Complete accepted moves | Logical sweeps | Final Y transitions | Kernel seconds | Process peak RSS bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4,096 | 16,384 | 32,768 | 20,478 | 259 | 1,024 | 0.004732 | 17,891,328 |
| 65,536 | 262,144 | 524,288 | 327,678 | 4,099 | 16,384 | 0.087005 | 18,268,160 |
| 1,048,576 | 4,194,304 | 8,388,608 | 5,242,878 | 65,539 | 262,144 | 0.724744 | 19,005,440 |

Every row passed the retained probe's output and matching audit. Exact
scalar-trajectory equivalence at these large sizes follows from the theorem;
the large probe did not enumerate all scalar stay visits or expand the dense
graph. The arbitrary-matching equivalence evidence is the separate finite
suite, not merely the special affine-permutation scaling generator.

The largest run had 184,549,328 logical read/write bytes, 176,160,720 peak
owned file bytes including prepared input and spools, and 159,383,504 retained
output bytes. Its input grew 256x relative to the smallest row while observed
peak process RSS grew about 1.062x. This supports investigation of the fixed
buffer schedule, not a claim that arbitrary four-million-vertex community
graphs fit in 19 MB. OS file-cache memory is not bounded by this RSS number.
The template represents a highly structured dense graph implicitly.

| b | SHA-256 of complete label bytes | SHA-256 of accepted-move bytes |
| ---: | --- | --- |
| 4,096 | `8e6f276f0b89b9e90359d61d1f95a15300cad9b45406c9ee3327e211848ace41` | `74fa61f787c7658b8e69dbab4765c38867bdb2a6120a711e775b2284393ff2cb` |
| 65,536 | `c331f4642c02a023ce0a8be50d531338ac05231d41e7c47bff80c16494d9a66c` | `a12a2b96cd2c3e91456ad63d5b150d693337e1314ce8bff09e04c3fe39abc4ef` |
| 1,048,576 | `2df2714ebb232e09ff3ce284a69ae7b5ed4f09c2f1863b31db6195dcc079843b` | `ca32d08f3ae6919ed869feecf6c8afaf04d70103b044375a0696311496dffd3c` |

Frozen implementation SHA-256:
`3a9432fb2131ad1272915e885cc341ba2ed743e810df6c1d0f351e662abe0e66`.
Six-test source SHA-256:
`584775b47fb71266d5ff813df64523a113c7fabea6983b111590cbb67b8a8bd7`.

## Independent Review And Closest Art

The completed [independent review](Communities-Monotone-Frontiers-Review.md)
supports the score-state, cursor, wrap, drain and label-attribution proofs.
The lead replayed its standalone checker, which imports no candidate code:
513 exact band/prefix cases; 50,363 generic permutation/schedule cases;
5,167 canonical crossing orders; 960 seeded arbitrary matchings; 16 expanded
singleton-start graphs; and seven unsafe-generalization witnesses. The
review compares exact gains as well as moves and labels. This is independent
mathematical evidence, not an independent audit of the packed implementation.

Its source ledger distinguishes full-text inspection from abstracts and
inaccessible articles. Particularly relevant precedents are:

- [Ding and Weiss, The K-D Heap (1993), primary preview](https://page-one.springer.com/pdf/preview/10.1007/3-540-57155-8_257):
  deleting a minimum under one of several priority orders is an established
  abstract operation. Our fixed, presorted, deletion-only case permits a
  much simpler representation; the abstract operation is not the novelty.
- [Akhremtsev, Sanders and Schulz (2014), Sections 4.1 and 5](https://arxiv.org/pdf/1404.4887):
  external label propagation already uses time-forward queues and sorted
  label recovery. We avoid those general mechanisms only after proving the
  special two-frontier execution and two-range output structure. The lead
  also retrieved this primary text; it is not our exact modularity schedule.
- [Yu, Srinivasan and Thomo, VLouvain (EDBT 2026), Sections 4-5](https://openproceedings.org/2026/conf/edbt/paper-72.pdf):
  community scores from inner-product features and community aggregates
  already avoid explicit dense edges. The lead inspected its feature model,
  degree/affinity formulas, aggregate updates and sweep-tolerance discussion.
  We must not claim that algebraic edge-free community scores are new.
  Our narrower proposed delta is a proved exact serial execution with a
  constant number of sequential buffers, not simply vectorized modularity.

There is also a representation distinction derived in the independent review,
not quoted from VLouvain. Restricted to Y, any exact Gram realization of the
matching off-diagonal weights, even allowing arbitrary diagonal entries,
has one independent 2-by-2 block per matching pair. Each block has rank at
least one, so feature dimension is at least b/2. A constant-dimensional exact
inner-product model cannot absorb these defects for free. This is **not** a
lower bound against sparse, shared-feature, low-rank-plus-corrections or other
specialized encodings. The matching itself still has a linear explicit encoding.

The current evidence therefore supports preserving this special reduction as
a candidate result. It does not settle whether the presorted multi-order
cursor lemma was previously stated, whether this family alone merits a paper,
or whether the same benefit occurs on customer graph data.

## Remaining Research Gate

This contribution changes a previously documented community scheduling-state
obstruction, not the other six families' status. Useful next work is a
broader, recognizable graph class retaining the least-live/frontier
property, or a useful real template-plus-corrections source on which the
paid workflow beats strong implementations. General Louvain compatibility,
automatic pattern discovery, physical caps and scientific priority remain
open. Do not count this theorem plus six existing manuscripts as seven
completed genuine innovations.

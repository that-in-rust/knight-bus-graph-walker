# Prefix-Encoded Discovery For Native Clique DFS

Date: 2026-09-21. A02 preparation/control study with a research executor,
nine passing focused tests and a completed, lead-replayed independent review.
This is a candidate space/work tradeoff, not a globally new DFS claim.

## Question And Chosen Design

The covered-edit connectivity review proves that a true base DFS forest is
a strong small-fragment comparator. Can that forest be built directly from
native overlapping groups without a resident visited bit per vertex?

Three choices are ordinary per-vertex discovery bits, disk-backed discovery
bits, or a source-position encoding. The first is simple and fast when it
fits. The second is a necessary baseline with random I/O. This study tests
the third: represent discovered vertices as the union of consumed prefixes
of the factor lists. Keep one cursor per factor and recover membership in
that union from paid occurrence-position rows. Spill the actual DFS stack;
do not hide recursion depth in the claimed RAM bound.

Unlike the A04 two-frontier result, this is a union of arbitrarily many
prefixes, not an intersection of two suffixes or a matching-specific label
executor. Only the general state-encoding idea is shared.

## Contract And Paid Views

Input is a fixed simple undirected graph defined by a union of F supplied
cliques. Original IDs are unsigned 64-bit integers; all original vertices,
including isolates, appear in a complete vertex stream. Factor lists have
unique members and fixed ordering. Duplicate factor identities, empty
factors and singleton factors are allowed. No pair edits are processed by
this forest builder; they belong to the separate covered-edit executor.

Required provider methods:

- `factor_count`, `vertex_count`: admitted nonnegative sizes.
- `factor_size(f)`: length of factor f.
- `read_factor_vertex(f,p)`: original vertex at zero-based position p.
- `read_vertex_occurrence(v,j)`: the j-th `(factor,position)` of v, or None.
- `iterate_complete_vertices()`: all original IDs exactly once.

Occurrence rows are the inverse of factor positions. Constructing,
validating, sorting, indexing and retaining them is paid preparation, not
an uncounted n-entry resident offset array. One lookup can cost a disk page.
The trusted provider is not a validator for hostile or inconsistent files.

Output is a stream of `(vertex,parent)` records in discovery order, with
None for roots. This is a true DFS forest, not canonical component labels,
a specified conventional adjacency-order DFS, or a full edited-graph solve.
Materializing output or deriving Euler/minimum indexes requires paid steps.

## Prefix Discovery Invariant

```text
Fixed factor lists                 Stored cursors

f0: [10 20 | 30 40]                p0 = 2
f1: [50 | 20 40]                   p1 = 1
          |
          +-- candidate 20 is not consumed here yet

Consumed-prefix union = {10,20,50}
20 is already discovered: its f0 position 1 is below p0.
40 is not discovered: both of its occurrences are unread.
```

The inverse row supplies the positions used by that test. It is paid
source data; the diagram does not assume a free per-vertex lookup table.

Let p_f be the first unread position of factor f. Immediately between
candidate-consumption steps, a positive-incidence vertex v is discovered iff

```text
there exists an occurrence (f,r) of v with r < p_f.
```

All cursors initially equal zero, so the set is empty. Before consuming
the candidate at `(f,p_f)`, test this predicate using its inverse row.
Then increment p_f. If v was already discovered, no new vertex is added.
Otherwise this newly consumed occurrence adds exactly v to the union,
and the algorithm immediately emits/discovers v. Induction proves the
invariant without an n-bit flag vector.

The order is essential: incrementing before the predicate would cause
every first occurrence to look previously visited. A root must also be
consumed from an unread factor position; arbitrarily marking an original
ID as a root without a consumed occurrence would break the encoding.
Incidence-free vertices are handled separately as isolated roots.

## DFS Execution

When a newly discovered vertex v has a parent, emit its parent edge and
push frame `(v,j=0)`. Its frame walks its occurrence row, obtaining each
incident factor f. Resume f at the globally shared cursor. For each
candidate, evaluate the prefix predicate before consuming it. Skip old
vertices; immediately descend to each newly discovered vertex, leaving
the caller's frame suspended at f. Advance the frame's occurrence index
only after that factor is exhausted.

If the stack is empty, scan for a remaining factor candidate and apply
the same predicate/consume rule to choose a new root. Finally stream all
original IDs and emit each incidence-free vertex as an isolated root.

The emitted parent edge is real: parent and child share the scanned factor.
Discovery/return nesting is depth-first. When v finishes, every incident
factor is exhausted, so all neighbors have been discovered. A neighbor
in a different earlier completed branch would have scanned their shared
factor before returning and discovered v there; thus no graph edge joins
incomparable DFS branches. These facts establish a spanning DFS forest,
not merely a traversal-shaped spanning tree.

## State And Work Target

Let n be vertex count, Z total memberships, d_v the number of factors
containing v and h maximum DFS depth. The source-position probe bound is

```text
P <= sum_v d_v^2
```

because v is a candidate d_v times, and each predicate scans at most d_v
occurrences. Early termination may be much cheaper. The graph-specific
extra cost must not be described as O(Z) on arbitrary overlap.

The execution target is O(F+B) resident words for factor cursors/sizes
and a bounded external-stack cache. The stack can require Theta(n) frames
on disk, even for a single clique. Output is also Theta(n). A two-block
cache avoids repeated disk traffic when push/pop alternates near a block
boundary; the file holds only spilled frames. Stack operations contribute
O(Scan(n)) block transfers amortized, but indexed source probes remain
separately charged and can dominate. No scan-I/O claim is made for the
complete algorithm.

With a trusted constant-cost random-access provider, a conservative work
bound is O(n+F+Z+sum_v d_v^2). Ordinary discovery-bit DFS needs no squared
membership probes. This is a tradeoff against that strong baseline, not
a simultaneous time-and-space improvement theorem.

### Sharp Review Correction

The [independent review](Connectivity-Prefix-DFS-Review.md) strengthens the
degree-only upper bound and supplies an attaining legal DFS family. For a
vertex of positive incidence degree d, number its fixed inverse-row entries
1 through d, and let r_i be their consumption order. Its exact predicate
work is

```text
P_v = d + sum_{i=1}^{d-1} min(r_1,...,r_i)
2d-1 <= P_v <= U(d) = d(d+3)/2 - 1.
```

The first candidate checks all d entries. Every later candidate stops at
the earliest already consumed entry. Among i consumed distinct ranks their
minimum is at most d-i+1. Consuming ranks in descending order attains U(d).
The review constructs actual factors and DFS order realizing that maximum,
not just a hypothetical permutation unavailable to the traversal.

For d>=3 its family has n=d-1, F=d, Z=3d-4, and total
P=(d*d+11*d-18)/2. At d=128 this means 380 factor-record reads but 8,887
predicate-record checks. The new saved test reproduces this family through
d=128 with reversed target occurrence order and explicit helper priorities.
This preserves the quadratic penalty, despite sharpening the upper bound.

If N vertices have positive incidence and c components contain such
vertices, the present implementation's exact total occurrence API calls are

```text
membership_record_reads = P + 2Z + 2N + n - c.
```

This includes row-end probes, suspended frame rereads and the complete-ID
pass. It is a useful logical tariff, not a count of physical disk pages.

## Implementation And Falsification Plan

Research files: `experiments/probe_prefix_cursor_dfs.py` and
`experiments/test_prefix_cursor_dfs.py`. No changes to production crates.

1. Write failing tests before implementation: complete original-ID forest,
   parent-edge validity, no cross edges between DFS branches, arbitrary
   factor order, repeated/empty/singleton groups and isolates.
2. Exhaust all two-factor incidence matrices on four vertices; add seeded
   overlap cases. Verify against an expanded-graph component oracle and
   the independent DFS ancestor characterization.
3. Add resource cases: a deep generated clique with constant source state,
   bounded two-block stack cache, exact source/probe counters, admission
   before source calls, and boundary push/pop/spill behavior.
4. Implement prefix-before-consume discovery and packed external frames.
   Run the focused suite. Correct failed assumptions, not the oracle.
5. Record receipts and explain what remains unpaid. A fixture-backed
   provider does not demonstrate a general packed source builder or RSS cap.

No publication-level novelty follows from a passing finite checker.
The closest-art comparison must include the DCC source in the parent
manuscript, space-efficient read-only DFS, and standard external stacks.

## First Execution Receipt

The initial seven tests failed with the intended missing-executor assertion.
After implementation, all seven passed in 0.222 seconds using Python 3.11.
Coverage includes all 256 two-factor incidence relations on four original
IDs, 500 seeded ordered-factor/overlap inputs, duplicate/empty/singleton
factors and isolates. The reference expands clique pairs and checks every
emitted parent, component coverage and DFS ancestor comparability on every
graph edge. For the exhaustive cases it also checks the cover-degree bound
for every vertex subset. This checks true DFS, not just a spanning forest.

The packed-stack test performs 5,000 randomized push/pop/update steps for
each of three block sizes, then drains each stack against a reference list.
The generated-clique tests validate every emitted parent online without
retaining outputs and confirm cleanup of the real temporary stack file.
They are logical/payload evidence, not measured whole-process RAM.

| Generated clique vertices | Resident factor cursors | Peak resident frames | Fixed frame buffer | Peak stack file | Total membership API calls |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 1 | 8 | 128 bytes | 0 | 47 |
| 10,000 | 1 | 8 | 128 bytes | 159,872 bytes | 59,999 |

These runs use four frames per block and two blocks in RAM. The larger run
writes and rereads 159,872 stack bytes. Its source is an arithmetic generated
clique, not an imported dataset. The fixed 128-byte payload excludes Python
objects, two factor arrays, counters, provider state, file/cache overhead
and the output consumer. It is not a 128-byte process or an enforced cap.
The test counts actual membership API calls, including end-of-row probes;
the narrower `visited_membership_checks` counter is 10,000, not 59,999.

Reproduce:

```sh
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover \
  -s research_algorithms_20260920/experiments \
  -p test_prefix_cursor_dfs.py -v
```

## Stronger Controls And Cost Corrections

Ordinary DFS with a bounded-cache disk discovery bitmap also has small
resident state. Together with factor cursors and an external stack it can
match O(F+B) resident words, while retaining about n/8 bitmap payload bytes.
Therefore removing per-vertex flags is a representation choice, not proof
that other DFS methods need n resident words or cannot fit the same budget.

The proposed predicate substitutes immutable source lookups for mutable
bitmap accesses. Which wins depends on cache locality, overlap and storage.
Using 64-bit fields, inverse `(factor,position)` tuples require 16Z payload
bytes; inverse factor IDs alone need 8Z. The extra 8Z can exceed the removed
n/8 bitmap by a large factor. If source positions are already native, that
incremental storage may be absent; if positions are derived on demand by
searching sorted factor lists, those searches must be paid instead. No
general storage or latency saving is established by this implementation.

The output callback is synchronous/backpressured but not transactional.
An exception cleans the stack file, not already emitted parent records.
A production snapshot publisher must stage, validate and atomically commit
the full forest and subsequent Euler/minimum indexes. Their bytes, time
and coexistence with the old snapshot are outside this prototype.

## Completed Independent Challenge

[The separate review](Connectivity-Prefix-DFS-Review.md) found no valid-input
DFS counterexample. The lead read its proof and replayed its standalone
checker: 135,850 ordered cases and 1,202,344 prefix-invariant snapshots,
with zero counterexamples and the six reported sharp-family counts.
The review additionally records 847 actual-probe executions and 13,728
stack histories; those are reviewer-executed receipts, not repeated here
or silently added to the independent checker's count.

It did find a real preflight gap. Invalid block size was checked only after
factor-array allocation and `factor_size` reads. The new
`test_buffer_preflight_order` first failed because a sentinel source was
accessed with `block_frames=0`. Validation now occurs before either factor
array is allocated or factor sizes are fetched. The nine-test suite then
passed in 0.232 seconds, including invalid zero/negative/bool/noninteger
settings and the added sharp-family regression. The review's recorded
source hashes describe the pre-fix revision, not a code audit of later edits.

Scratch cleanup and partial-output caveats remain unchanged. This closes
the identified admission-order defect, not OS-level resource enforcement,
source consistency validation or transactional publication.

Frozen post-fix implementation SHA-256:
`c271915b92a4e42637eeeffc800766981dae247af67da1de19e609e0a5370d2a`.
Post-fix tests SHA-256:
`92f1a963c05b465685fd75591bb27eff99b57219f1027605d613fc2db80b0783`.
Replayed independent review SHA-256:
`27b2e78c5d4369d23c1d98a1bc7543011513045d584e48b13392421443f5b88a`.

## Inspected Context

The [parent manuscript's DCC full-text comparison](Connectivity-Fault-Cover-Quotient.md#dcc-full-text-follow-up)
records the direct representation-aware DFS predecessor. Do not attribute
the source-prefix predicate, cost bounds or this implementation to that
paper without an explicit matching passage.

[Hagerup, 2018](https://arxiv.org/abs/1805.11864) gives space-efficient DFS
time/bit tradeoffs for general graphs. The lead inspected its primary
abstract, not the full proof; it prevents treating ordinary n-word DFS
as the only serious RAM baseline.

[Elberfeld, Kammer and Meintrup, ISAAC 2025](https://drops.dagstuhl.de/storage/00lipics/lipics-vol359-isaac2025/html/LIPIcs.ISAAC.2025.29/LIPIcs.ISAAC.2025.29.html)
studies prepared succinct encodings for separable graphs and DFS recorded
inside that encoding. The lead inspected the abstract and introduction.
Its model/output is different from streaming every original parent here;
its sublinear claims cannot be copied into an explicit all-vertex export.
These sources establish important alternative research directions, not
priority for our candidate. The completed separate review additionally
records inspected full-text Hagerup passages and the exact source-position
comparison. Prepared native-source applicability, a bounded builder and
fair bitmap measurements remain open.

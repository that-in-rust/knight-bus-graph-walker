# Connectivity After Covered Edits Without Vertex-Wide State

Date: 2026-09-21. A02 follow-on. Exact prepared-input research executor and
finite evidence; not a production memory cap, a general graph importer, or
a claim that the graph-theoretic ingredients are globally new.

## Result And Why It Matters

An analytical graph may be supplied as overlapping groups: two entities are
adjacent when they share a group. Expanding every group into a clique is
unnecessary for ordinary connectivity; that is established incidence-native
execution and already D06 in our architecture notes.

This follow-on permits explicit missing and added relationships. Instead of
retaining a union-find entry for every entity, retain one node per group plus
one per vertex in a **cover of the changed edges**. If F is the group count
and k the cover size, the prepared executor uses O(F+k) working words,
independent of the vertex count n and total edit count delta. Input, output,
indexes and preparation are still paid and can be much larger.

The cover can be small even when many relationships change. For example,
all deletions incident to one entity have a one-vertex cover. This does not
mean those deletions can be ignored or that only one record is read.

Two refinements avoid some unnecessary work:

1. A degree-floor check can prove a cover vertex still reaches a group
   without reading its deleted neighbors' membership rows.
2. Core-to-core edges need enumeration only from **blocked incidences**:
   cover-vertex/group pairs having no surviving neighbor outside the cover.
   When both endpoints reach the group's outside clique, their connectivity
   is already represented and their direct edge need not be processed.

These are a specific exact execution reduction, not an assertion that
quotient graphs, maximal matching, pigeonhole reasoning or complement-graph
algorithms were invented here. The closest-art review is a separate gate.

The completed independent review supports the construction but supplies a
stronger countercontrol: an immutable true DFS forest of the same clique
union also admits O(F+k) fragment state after covered edits. Therefore the
surviving comparison concerns paid preparation, retained indexes and access
work, not an exclusive asymptotic query-RAM advantage. See the proof below.

The subsequent [capped complement schedule](Connectivity-Cover-Complement-Study.md)
improves the worst-case choice of core processing with the same workspace
order. Its local attempts plus one shared fallback have a proved sum-of-
minima work bound. The [execution evidence](Connectivity-Capped-Execution-Evidence.md)
now integrates it as `core_plan="capped"`, with ordinary `complement` and
original `blocked` controls, a streamed SQLite prepared source and full-ID
output checks. Logical query-state and measured process RSS are distinct
from a whole-host cap. [Prefix-encoded DFS](Connectivity-Prefix-Visited-DFS.md) separately
implements a preparation/control option and documents its sharp overlap
penalty and disk-bitmap alternative.

## Contract

Let V be a fixed universe of n distinct original vertex IDs, including
isolates. There are F supplied sets S_f, each with unique members in V:

```text
G0 = (V, union_f complete_graph(S_f))
G  = (V, (edges(G0) minus D) union A).
```

The union is Boolean adjacency, not a sum of independently live group-edge
copies. D and A are normalized simple undirected pairs, with no loops,
duplicate messages, or stale insertions. Here A is non-base and disjoint
from D. A missing pair is missing through every group that formerly implied
it. Partial deletion of parallel source relationships is normalized first;
it is not a deletion here until the canonical relationship becomes absent.

Return every vertex with the minimum original ID in its current connected
component. Direction is ignored under this WCC projection. This is not SCC,
shortest paths, maintained reachability on a directed graph, or a spanning
forest exporter. Virtual factor unions alone are not original-edge witnesses.

Let C be a vertex cover of the graph (V,D union A): every changed pair has
at least one endpoint in C. Write k=|C|. The letter C here denotes the cover,
not a component or a stored sparse certificate. The whole snapshot and cover
remain immutable for an execution.

## Obtaining The Cover

A supplied cover is checked against the complete normalized edit stream.
Otherwise use the established maximal-matching endpoint algorithm: scan
edits, and whenever neither endpoint is already selected, select both.
The selected edges form a matching; every later edit touches a selected
endpoint or causes selection. The result is a cover of size at most twice
the minimum possible cover. Computing an exact minimum cover is not assumed.

The implementation refuses before adding a pair that would exceed its cover
reservation. A refusal does not prove that no smaller cover fits. In a star,
the greedy cover has two vertices, whereas a supplied center-only cover has
one. Sorting or removing redundant chosen vertices can be separate work;
neither is hidden in the approximation claim.

## Quotient Construction

For each factor f define:

```text
T_f = S_f minus C                outside members
m_f = |T_f|
C_f = S_f intersect C            cover members
t_f = |C_f|.
```

All edges among V minus C are unchanged because the cover touches every
edit. Therefore each nonempty T_f is an intact clique. Give it a virtual
factor node. The implementation reserves F slots conservatively; factors
with m_f=0 remain unused as outside nodes. Give every s in C its own node.

### Phase 1: Join The Outside Factors

Stream original vertices. For each v outside C, union every incident factor
with the first one in its membership row. Update the resulting component's
canonical minimum with v. A vertex may belong to many overlapping groups;
the stream must include all of those memberships.

This is the standard connectedness equivalence between a hypergraph's
incidence graph and the union of its clique expansions. It creates no
edge through a missing relation: outside-outside relations cannot be edited.

Vertices with no memberships are not allocated union-find nodes. An
unaffected outside isolate can be returned with its own ID during output.

### Phase 2: Reattach Each Cover Vertex

For s in C and f containing s, define

```text
d_sf = |N_D(s) intersect T_f|.
```

There is a surviving base edge from s to the outside clique of f exactly
when `m_f > d_sf`. In that case union s with f. Do not substitute s's
global deletion degree for d_sf in this exact test: deletions through a
different factor say nothing about which members of this factor remain.

### Phase 3: Keep Necessary Cover-To-Cover Connectivity

For every surviving base pair s,t in C, some factor contains both. Add that
edge to the quotient unless its connectivity is already represented by
outside attachments, using the blocked-incidence rule below. Explicit
cover-cover insertions are also unioned.

### Phase 4: Insertions Touching Outside Vertices

An inserted pair cannot have two endpoints outside C. When streaming an
outside vertex v, consume all its inserted neighbors, which lie in C.

- If v has a factor, union those cover nodes into that factor's component.
- If v has no factor, union its inserted cover neighbors with one another
  and include v in their canonical minimum. No resident node for v is needed.
- If v has neither memberships nor insertions, leave it implicit.

The implementation performs this work during Phase 1, before reattachment;
the union operations commute at the partition level. An originally isolated
outside vertex can connect two different cover components and can be their
smallest ID. Dropping it from the minimum or treating its inserted edges
independently without unioning their endpoints would return a wrong answer.

## Exactness Theorem

The quotient and G induce the same partition on all represented original
vertices, with unconnected outside isolates remaining implicit singletons.

**No invented connectivity.** Every factor denotes a nonempty intact
outside clique. Sharing an outside vertex provides a real connection
between factors. Each cover-factor attachment has at least one actual
surviving edge by its strict count inequality. Processed cover-cover edges
and insertions are actual current relationships. Suppressing an outside
isolate joined by insertions merely contracts its real star of edges.

**No lost connectivity.** Consider any current edge. An outside-outside
edge lies in a factor whose outside clique is represented. A surviving
base cover-outside edge causes its cover endpoint to attach to that factor.
A surviving cover-cover edge is either explicitly processed or has its
endpoints already connected through the same outside clique, as proved
below. Every insertion is handled by its cover-cover or cover-outside case.
Thus replacing edges along a path preserves connectivity.

All component minima are exact because every cover ID initializes its own
node, every represented outside vertex contributes its ID during the first
scan, and each successful union preserves the smaller minimum. The final
vertex scan emits its cover root, any incident outside-factor root, an
inserted cover-neighbor root for an original isolate, or its own ID.

## Refinement A: Degree-Floor Attachment

First stream the deletion row of s to mark deleted cover neighbors and
count only its deleted outside neighbors:

```text
d_out(s) = |N_D(s) minus C|.
```

For every incident factor with `m_f > d_out(s)`, attachment is certain,
since `d_sf <= d_out(s)`. The strict inequality matters. No deleted-neighbor
membership lookup is needed for these factors.

Factors with `0 < m_f <= d_out(s)` are uncertain. If there are any, reread
s's deletion row and look up each deleted outside neighbor u's factor row.
Increment a counter only for factors in this uncertain set. Direct-indexed
generation stamps and counters use O(F) words; no table of all projected
deletion/factor occurrences is retained. The number of scanned membership
records can still be large and is charged below. Empty outside factors
cannot attach and need no such lookup.

This implementation pays an extra deletion-row pass for uncertain vertices.
The guard is not free latency improvement; its value depends on avoiding
enough membership work.

## Refinement B: Enumerate Only Blocked Incidences

Call (s,f) blocked exactly when s belongs to C_f and has no surviving
neighbor in T_f. This includes all incidences with m_f=0. Let b_f be the
number of blocked cover members of f.

**Rule:** scan the prepared cover posting C_f only for blocked (s,f).
For each t encountered, exclude self and deleted pair (s,t), deduplicate
with a reused generation array, and union the remaining candidate pairs.

**Proof.** Let a surviving cover pair s,t be implied by f. If neither
incidence is blocked, both endpoints already attach to f and hence are
connected through its intact outside clique. Otherwise at least one of
the blocked endpoint rows scans C_f and processes the pair. No assumption
about the order in which s and t are processed is needed: union operations
preserve connectivity regardless of which endpoint's attachment occurs
first. A deleted pair is never added by this rule.

The core posting work is therefore

```text
C_block = sum_f t_f * b_f,
```

rather than the initial draft's `sum_f t_f^2`. Epoch-stamped forbidden and
candidate arrays, plus one touched-candidate list, avoid an obligatory
k-by-k matrix or a k-element reset/scan on every cover row. The candidate
list has at most k-1 entries, even when many factors imply the same pair.

For m_f>0, each blocked member has deleted all m_f outside relationships.
Thus `b_f*m_f` is at most the number of projected deleted cover/outside
incidences in that factor. This is a charging observation, not a claim
that projection multiplicity is bounded by |D|. Core-only factors m_f=0
still have b_f=t_f and can require quadratic posting work in this plan.

## Prepared Access Layout

The prototype operates against an explicit provider interface, not a hidden
in-memory graph created inside the executor:

| View | Required semantics | Storage/access obligation |
| --- | --- | --- |
| Complete edit stream | Every normalized pair in D and A | Cover construction/validation reads it |
| Vertex-major rows | Every original vertex, all unique factor IDs, all insertion neighbors | Two complete streaming passes, including isolates |
| Indexed vertex membership row | Unique factor IDs for a requested original vertex | Paid on-disk lookup and row scan; no n-entry resident offset array assumed |
| Factor-major cover postings | C_f for each factor | Query-cover-specific materialization or equivalent indexed view |
| Cover-vertex deletion rows | Complete unique deleted neighbors of s | One pass, sometimes a second pass |
| Cover-cover insertion stream | Every insertion with both endpoints in C | Separate union pass |

The cover-specific factor postings require Z_C=sum_f t_f records. A
bounded builder can scan selected cover vertices' membership rows, emit
(factor,cover-ID) records, and externally sort them. This costs source
lookups, writes, sorting and scratch; it is not performed for free by the
query engine. The deletion and insertion views also require complete
normalization, sorting/indexing and snapshot identity checks.

The existing `build_incidence_row_store` in the PageRank experiments was
inspected through codebase-memory. It explicitly allocates n resident row
lists and degrees. It is not reused as evidence of an O(F+k)-RAM builder.
The new tests use resident fixtures for verification and a lazy generated
source for a state-bound case. A general packed builder/provider is not
implemented here. Neither fixture memory nor caller-retained outputs count
as proof of whole-workflow RAM compliance.

## RAM And Work Accounting

Working state comprises union parents, sizes and canonical minima for F+k
nodes; three reusable F-sized count/stamp arrays; cover lookup and O(k)
candidate/forbidden/seen arrays; one cover membership row of at most F
entries; a touched-candidate list of at most k; and bounded provider/output
buffers. No n-sized label vector, |D|-sized edge set or k^2 table is needed.

The `max_state_nodes` parameter reserves DSU nodes only. It is deliberately
not named a byte limit: Python object overhead, I/O buffers, source-provider
state and OS cache require separate admission and measurement. IDs and
counters also have finite word widths; arbitrary-size integers are not
constant-byte objects.

Define Z=sum_f |S_f|, delta=|D|+|A|, and let U be the cover vertices with at
least one uncertain nonempty outside factor after the degree-floor test.
The implemented negative membership work is

```text
J_U = sum_{s in U} sum_{u in N_D(s) minus C} |factors(u)|.
```

With constant-cost ID lookup, a convenient upper bound is

```text
O(n + F + Z + delta + J_U + C_block
  + (Z + |A| + C_block + n) * alpha(F+k+1))
```

plus sorting the supplied cover and the separately paid preparation. The
bound counts record/arithmetic work, not wall time. The Python prototype
uses hash lookup for cover IDs; worst-case deterministic lookup can instead
use sorted cover IDs and binary search, charging log(k+1) per such lookup.
It is not valid to claim worst-case hash-table latency from this prototype.

Important access qualifications:

- J_U can greatly exceed |D| when a deleted neighbor belongs to many groups.
- The number of indexed membership lookups is also paid independently of
  the returned row bytes; cold random pages need not behave like a scan.
- C_block can equal sum_f t_f^2 when factors have no outside members.
- Full original-ID output is Omega(n), even if the working quotient has
  only two nodes. Output is streamed through a callback, not retained.
- A source with F comparable to or larger than n may favor ordinary vertex
  DSU instead. Refusing the selected reservation is not an impossibility
  theorem for other representations.

## Draft One Versus Draft Two

The first executor was correct on the complete small snapshot suite but
unnecessarily scanned every cover posting for every incident cover member
and looked up every deleted outside neighbor's memberships. It also scanned
k candidate flags on every row.

Two new resource-contract tests deliberately failed that version:

| Fixture | Draft-one core records | Required corrected records | Corrected negative-membership records |
| --- | ---: | ---: | ---: |
| One six-vertex group; cover {0,1}; delete (0,2),(1,3) | 4 | 0 | 0 |
| One seven-vertex group; cover {0,1,2}; vertex 0 fully blocked | 9 | 3 | 4 |

The second fixture deletes (0,1),(0,2),(0,3),(0,4),(0,5),(0,6),(1,3),(2,4).
Vertex 0 is isolated; all others have canonical component label 1.
The corrected algorithm uses the degree floor, blocked-incidence proof and
epoch-stamped touched candidates. Both new tests and all prior correctness
tests now pass. This is a real algorithm revision, not a renamed passing test.

## Falsifiers And Stronger Controls

1. **Global degree is not a local missing count.** With factors {0,1,2}
   and {0,3,4}, deleting (0,1),(0,2) isolates vertex 0 from the first factor
   but leaves its second factor intact. Its global deletion degree is two
   in both comparisons; only local counts distinguish the outcomes.
2. **A factor can have no outside representative.** A group wholly inside
   C cannot be treated as an intact clique after deletions. It contributes
   only its surviving core edges.
3. **Overlapping factors do not restore globally deleted pairs.** Two
   identical {0,1} factors still yield two isolates after deleting (0,1).
4. **Outside isolates can become bridges and minima.** With no factors,
   cover {5,9} and insertions (0,5),(0,9), all three labels must be 0.
5. **A huge deletion star is not a universal sensitivity-oracle win.** A
   star-shaped base witness tree loses all its edges, but a path witness
   tree in the same clique loses at most two edges at the star center.
   The latter also has constant fragment state. Do not compare only to
   the deliberately bad witness tree.
6. **Small state does not imply small work.** Core-only large factors can
   make C_block quadratic; known complement-graph traversal can be better.
   Heavy membership overlap can make J_U large despite few edit records.
7. **Ordinary no-edit incidence connectivity already uses factor state.**
   k=0 is the established D06 baseline, not the new part of this proposal.
8. **Finding the source structure is a separate problem.** This accepts
   native group memberships. It does not discover a small clique cover of
   an arbitrary Neo4j graph at no cost.

### General DFS Countercontrol

The single-clique path example is only a special case. The independent
review derives the following stronger control for every union of F cliques.
Choose any true undirected DFS forest T of the base graph before seeing
the edits or cover. Let l be the number of rooted leaves in its nontrivial
trees and c the number of those trees.

Distinct DFS leaves are incomparable, and an undirected DFS has no edge
between incomparable vertices. Consequently two such leaves cannot share
a clique factor. Each leaf belongs to a factor witnessing its parent edge,
so choosing such a witness injects the leaves into the factors: l <= F.
For child count ch(v), the rooted-forest identity and degree bound give

```text
sum_v max(ch(v)-1,0) = l-c
deg_T(v) <= 2 + max(ch(v)-1,0)
sum_{s in C} deg_T(s) <= 2k+l-c <= 2k+F
d_T = |D intersect edges(T)| <= 2k+F.
```

At most F base components have a factor membership. Incidence-free outside
isolates participating in insertions can be eliminated as in our kernel;
at most k incidence-free cover vertices require explicit nodes. Counting
all factor-supported fragments therefore yields

```text
q <= d_T + F + k <= 2F + 3k.
```

This is a derived comparator theorem, not a theorem attributed to the DCC
paper below. It defeats universal mutable-state superiority even for
overlapping groups and adversarial edits chosen after the forest. It does
not supply surviving replacement edges, a bounded builder or free Euler
indexes. Nor does it prove that every forest implementation must retain
an n-entry ownership array. A fair experiment must give the competitor
native factor access and account for both implementations' preparation,
retention, repair and all-ID output.

## Verification Receipt

Seven missing-module assertions were observed before the first implementation.
Seven initial tests then passed in 0.287 seconds. The two sharper work tests
failed with 4-versus-0 and 9-versus-3 posting counts before the refinement.
The final nine-test run passed in 0.287 seconds and covers:

- All 256 incidence relations on four vertices and two factors, crossed
  with all 64 current simple graphs: 16,384 complete canonical partitions.
  D/A are derived from the base/current difference; no warm-start answer is
  supplied to the candidate.
- 500 seeded cases with arbitrary original IDs and deliberately nonminimal
  covers, including empty factors and core-only execution.
- All 1,024 simple edit graphs on five vertices, checking the greedy cover
  against an independently enumerated exact minimum cover.
- The factor-local count, duplicated-factor deletion, original-isolate
  insertion bridge, and work-refinement falsifiers above.
- A lazily generated single-clique deletion star at n=8 and n=10,000 with
  exactly two union-state nodes and complete streamed label checks. This
  is logical/provider evidence, not a disk or process-memory measurement.
- Cover/reservation errors, including refusing DSU allocation before any
  source method is called when F+k exceeds the selected node reservation.

The [completed independent review](Connectivity-Fault-Cover-Review.md) was
read and its standalone checker replayed by the lead on 2026-09-21 using
Python 3.11. It exited 0 with 10,191 exhaustive snapshots, 85,530
kernel/label/work/DFS-cover checks, 20,382 greedy-cover checks and 3,000
seeded cases. Eleven faulty shortcuts and three invalid source/cover cases
were rejected. It independently constructs its candidate and expanded
oracle; replay is confirmation of that checker, not a third implementation.
Its coverage includes every valid edit cover on the enumerated small inputs.

Reproduce:

```sh
/Users/amuldotexe/.local/bin/python3.11 -B -m unittest discover \
  -s research_algorithms_20260920/experiments \
  -p test_fault_cover_connectivity.py -v
```

## Prior-Art Boundary And Remaining Gate

Primary sources inspected by the lead already rule out broad claims:
[Dahlhaus, Gustedt and McConnell (2002), Theorem 1](https://dmtcs.episciences.org/303/pdf#page=4)
supports graph searches in partially complemented representations without
expanding all represented arcs. Summary-plus-correction representations
are also established; [Chu et al., Section 2](https://nedchu.github.io/sigmod2024.pdf)
describes that encoding framework. Avoiding complement expansion or storing
exceptions is not, by itself, an innovation of this manuscript.

The [independent fault-cover review](Connectivity-Fault-Cover-Review.md)
identifies annotated torsos as the closest structural predecessor:
[van Bevern et al., Section 2.3.1](https://arxiv.org/pdf/1312.7014v2#page=13)
contracts outside components while preserving original-vertex ownership.
The proposed execution computes the same kind of connectivity reduction
through the supplied incidences and avoids realizing all quotient edges.
It does not invent the underlying contraction principle.

### DCC Full-Text Follow-Up

On 2026-09-21 the lead retrieved and inspected
[Ullah and Pothen, arXiv:2604.28096v1](https://arxiv.org/pdf/2604.28096):
Theorem 1.1, Section 2, Section 4.2/Figure 7/Lemma 4.8,
Section 4.3/Figure 8/Lemma 4.9, and Appendix D.3-D.4.
This closes the review's abstract-only access gap for those passages, not
for every claim in the 53-page manuscript. Its model excludes isolates.
Its DFS uses shared clique cursors, vertex discovery indicators and parent
pointers; its union-find connectivity initializes every vertex. It gives
incidence-size search bounds. Dynamic maintenance and memory-scarce
execution are explicitly future directions. These are serious precedents,
not evidence that our particular edited-snapshot schedule is already
proved there. Its published experimental gains are not our measurements.

The next contribution question is whether a complete paid implementation
of the covered-edit schedule improves useful workloads against native
DFS/fragment and complement-search controls. Logical O(F+k) workspace,
finite correctness evidence and absent keyword matches alone do not prove
publication-level novelty or a physical RAM/latency gain.

This is progress on an exact, parameterized working-state architecture and
a graph-specific work-elimination rule. Native-source prevalence, complete
paid preparation, physical memory/latency, and a defensible scientific delta
remain open. The full seven-family goal is not complete.

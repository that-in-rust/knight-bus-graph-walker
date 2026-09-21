# CluStRE Source: What Streaming Actually Removes

Date: 2026-09-21. A04 continuation. Status: pinned source inspection, not a
build, runtime measurement, bug report, or proof that our alternative is better.
Read with the independent [paper audit](Communities-CluStRE-Paper-Audit.md).

## Decision

The next low-RAM contribution should target a particular state term, not the
word "streaming". The inspected implementation avoids keeping all original
adjacency resident in its streaming path, but retains node assignments, cluster
metadata, a whole input row, and, in quotient modes, a potentially dense coarse
graph. Local refinement adds active-node sets and random row access.

The most promising research target from this inspection is a **budgeted
quotient representation with an original-graph quality certificate**. This
would be a different quality/resource contract from preserving a particular
Louvain or CluStRE move trajectory. It does not replace the previous exact-replay
track, and it is not yet an established novel algorithm.

## Reproduction And Scope

- Official repository: <https://github.com/KaHIP/CluStRE>.
- Inspected commit: `39a4e57c2de0bee45a2752597db502963a1536de`.
- Local ignored checkout, root-relative: `gitrefrepo/CluStRE-src`.
- Sparse checkout includes root files, `app`, `lib`, and `eval_scripts`.
- MCP project `clustre-source-audit`: 874 nodes and 2,982 edges at indexing.
  The index excluded `lib/tools`; absent graph edges are not evidence of absent
  calls. Explicit main-program calls were checked in source.
- No compile, dependency resolution, author-result reproduction, timing or RSS
  experiment was performed. External VieClus optimizer allocations are not
  covered by this audit.
- `CMakeLists.txt:85-90` fetches CPI at moving `origin/main`. A reproducible build
  would need that dependency pinned as well. The root commit alone is not a
  complete dependency lock.

All source links below are immutable GitHub permalinks at that commit. Asymptotic
storage statements are our deductions from those allocations, not author
benchmark results or measured allocator footprints.

## Execution And State Lifetimes

```text
Input row + earlier assignments
              |
              v
Streaming assignment -----> quotient pair map (selected modes)
              |                         |
              |                         v
              |              temporary adjacency -> VieClus graph
              |                         |
              +--------- remapped labels+
              |
              v
Restream -> active sets -> random-row local refinement
              |
              v
Recorded time / RSS -> output -> optional score evaluation
```

`app/clustering.cpp:180-285` reads one node row per main iteration. Quotient
updates occur only at `restreaming == 0` and outside LIGHT/LIGHT_PLUS. The code
then invokes the selected external clusterer and performs the configured
restream passes. [Main streaming lifecycle](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/app/clustering.cpp#L180)

### Storage Ledger

Let n be input nodes, M stored undirected input edges, K created cluster IDs,
q distinct nonzero unordered quotient pairs including self-pairs, Delta the
largest input row degree, and s the text-row checkpoint interval. Count records
first; byte widths, capacities, allocator overhead and cache require measurement.

| Term | Source evidence | Deduction and scope |
|---|---|---|
| Node assignments | `readFirstLineStreamClustering`, default `rle_length == -1` | Allocates n `PartitionID` entries; O(n) resident default mapping |
| Cluster arrays | `solveClustering` appends blocks, neighbor slots and cluster-to-slot mappings | O(K) metadata even when one node touches few clusters |
| Parsed input | Text `getline` followed by integer parsing; binary row buffer followed by vector insertion | O(Delta) row residency, with overlapping encoded/decoded representations |
| Quotient | `update_quotient_graph` inserts or updates unordered pair keys | O(q) hash entries; q <= min(M, K(K+1)/2) for positive, undirected normalized input |
| Quotient conversion | Pair map to Q adjacency; Q to `graph_access` | Transitional representations coexist; erasing an entry is not proof that table capacity returned to the OS |
| Active frontier | Old active hash set and new active hash set in local search | Both can be live; O(n) logical entries across each frontier in the worst case |
| Text row checkpoints | `partialOffsets`, recorded during restreaming == 1 | O(n/s) offsets; skipping at most s-1 rows is not a byte-I/O bound |
| Optimizer | `perform_partitioning` invoked on constructed coarse graph | Additional state unknown in this sparse source audit |
| Output/evaluation | Executed after stored runtime and RSS values are captured | Must be included separately in our end-to-end comparison |

Assignment allocation and maximum-cluster default:
[graph_io_stream.cpp:143](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/lib/io/graph_io_stream.cpp#L143).
Cluster creation and scoring:
[vertex_partitioning.h:204](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/lib/partition/onepass_partitioning/vertex_partitioning.h#L204).
Quotient insertion:
[vertex_partitioning.cpp:43](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/lib/partition/onepass_partitioning/vertex_partitioning.cpp#L43).

The q <= M bound concerns distinct stored coarse pairs, not the number of hash
updates or total weight. A configured cluster-count bound does not make q
linear in K. Nor does it make the O(n) assignment vector constant-sized.

### A Hub Is Not A Constant-Sized Buffer

The text loader first owns the full row string, then parses it into the node's
integer vector. The binary random-row loader allocates the raw edge array and
then fills a vector before releasing that array. `num_lines = 1` limits row
count, not row bytes. [Text loader](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/lib/io/graph_io_stream.h#L335),
[random-row loader](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/lib/io/graph_io_stream.h#L221).

A bounded replacement must tokenize or decode in fixed-size pieces. It must
also solve the next problem: accumulating affinity by neighboring community.
Splitting the input row while building an unbounded community hash map simply
moves the memory failure. Exact external aggregation, repeated scans, and
bounded sketches with certified fallback are distinct alternatives.

### Quotient Conversion Is Part Of Peak Memory

`convert_q_to_local_ds:66-133` creates Q with K vector headers. It inserts map
entries into Q while erasing map entries, then deletes the map. It next allocates
the VieClus graph and constructs directed adjacency, clearing Q rows as it goes.
The correct description is **overlapping lifetimes**, not an assumed three-full-
copies multiplier. Capacity behavior and external graph allocation still need
measurement. The conversion also explicitly rejects coarse sizes exceeding its
32-bit conditions, despite root 64-bit build options.
[Conversion source](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/lib/extclustering/extclustering_vieclus.cpp#L66)

### Refinement Does Not Bound Bytes By Counting Rows

For text input, local refinement seeks to the checkpoint at
`floor(node_id/s)*s`, then reads through the target row. The default s is 10.
A checkpoint neighborhood containing a hub can cost much more I/O than ten
small rows. Active nodes are iterated from a hash set, not our increasing-ID
serial order. Time-limit checks occur after a node's complete processing.
This is not a bounded per-chunk wall-clock guarantee.
[Offset lookup](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/lib/io/graph_io_stream.h#L264),
[local search](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/app/clustering.cpp#L298),
[configuration](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/app/configuration.h#L181).

### Output Boundary

`total_time` is captured at line422 and `getMaxRSS` at line430. Writing labels
occurs at lines461-467 and optional evaluation at469-475. The FlatBuffer stores
the earlier values. This source ordering does not invalidate the authors'
chosen experiment; it means our customer-workflow comparator must wrap the
complete process rather than reuse those values as end-to-end measurements.
[Reporting boundary](https://github.com/KaHIP/CluStRE/blob/39a4e57c2de0bee45a2752597db502963a1536de/app/clustering.cpp#L422)

## Three Opportunities And Their Strong Controls

### Reconciling Paper And Source

The independent paper audit identifies a stale-edge risk in the printed
forward-neighbor quotient pseudocode. Do not apply that finding to this source
without qualification. In the inspected uncompressed first pass, assignments
start INVALID, `readNodeOnePassClustering:174-198` filters INVALID neighbors,
and the main program commits the current node's assignment once. Thus only
already assigned neighbors contribute to the quotient; keys are canonicalized
by min/max. For normalized loopless undirected input, each edge is committed
when its later endpoint has its phase-final decision. This is the ordinary
correct edge-commit invariant, unlike the literal printed pseudocode.

This reasoning is conditional on the first-pass path just inspected, not a
claim that every score, mode, weighted-input path, or external optimizer was
runtime-validated. The paper's singleton initialization and the source's
INVALID-until-arrival convention are different enough that exact first-pass
trajectory equivalence must not be assumed.

| Opportunity | Ordinary control that must be credited | What would constitute a stronger contribution |
|---|---|---|
| Bound quotient storage | External sort/coalesce and external adjacency; coarse-edge sparsification | A compact representation with a useful original-objective certificate and demonstrably better quality/resources |
| Bound hub-row affinity state | External group-by; repeated exact scans; weighted frequent-item sketch | A tighter certified candidate rule or work bound that avoids substantial spill without silently losing the winning move |
| Reduce label/frontier residency | Disk-backed arrays, bitmaps, external queues, compressed assignments | A workload-relevant sufficient statistic or algorithm that removes state, rather than hiding it in caches |

The [2025 linear-time multilevel partitioning paper](https://arxiv.org/html/2504.17615v1)
already enforces geometric coarse-edge reduction through sparsification and
analyzes cases in which contraction alone is insufficient. Its objective is
balanced graph partitioning, not our fixed original-graph modularity contract.
Nevertheless, coarse-graph sparsification is plainly established prior art;
renaming it would not be a contribution. Inspect sections4-5 when developing
the quality certificate. The arXiv template's placeholder conference metadata
must not override the [ESA2025 publisher record](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ESA.2025.32).

## Research Contract Selected From This Audit

Study a fixed initial partition P0 and a bounded representation of its coarse
graph. Separate three guarantees:

1. Same original-graph objective: retain original degrees and normalization;
   do not score only the sparsified graph and call it the same objective.
2. Quality: certify an additive loss, a safe improvement over P0, or exact
   decisions under stated margins. These are different deliverables.
3. Resources: include mapping, construction, refinement, spill, output and
   validation. Bounding the retained edge count alone bounds none of the others.

First falsify the simplest alternatives. Derive the ordinary cut-sparsifier
bound with original degrees. Derive a one-sided deletion certificate for
merge-only refinement. Test counterexamples for changing the degree tax,
allowing arbitrary splits, and claiming an optimum from a heuristic. Only then
decide whether a sharper representation or certificate has a research delta.

Follow-on: [Residual-Degree Quotient Envelopes](Communities-Residual-Degree-Envelopes.md)
now supplies a concrete bounded-counter builder, original-objective interval,
finite exact-rational checks and an indistinguishable-input obstruction. The
new direction is a quality/resource experiment, not a replacement claim for
the exact two-label solver.

## What This Audit Does Not Establish

- No statement that CluStRE is broken or that its modes have identical semantics.
- No exact RSS multiplier or runtime comparison.
- No proof of fixed-budget completion from streaming alone.
- No proof that run-length compression is small for arbitrary ID order.
- No equivalence between its active-frontier heuristic and our exact serial
  local-moving semantics. Those require separate contracts and tests.
- No theorem of globally novel community detection. The concrete progress is
  locating source-backed costs and defining stronger baselines for the next
  representation, not proclaiming an eighth name for a known technique.

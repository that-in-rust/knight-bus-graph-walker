# Similarity: Larger Groups Must Pay For Their Information

Date: 2026-09-21. Predeclared logical workload gate, not a timing, RAM or
production-layout experiment. Execution and independent scoped review complete.

## Question And Fixed Design

The reviewed compressed-dual theorem removes the original G-leaf tree from
repeated exact-envelope evaluation, but it has not demonstrated useful savings
on the larger-G public workloads. Test that missing gate before implementing
another disk layout. The existing G=4 interval and DAAT controls remain strong.

Use the exact source gzip hashes, physical original-ID consecutive pairs,
thirty source IDs per dataset, k=10 and feature-ID modulo G from the retained
GrQc/Facebook overlap receipts. Evaluate G=4,16,64. G changes the observations;
its binary tree need not be a nested refinement of the previous modulo tree,
so no monotonic tightening assumption is allowed.

For each query, simulate the existing relevant-block traversal and exact
score/ID top-k ordering with union, interval, modal-branch and modal-dual
plans. Both modal plans run only after the cheaper filters cannot prune.
Prepare their static modal summaries once per source/group profile, using
tau=8. A query exceeding a block's tau profile falls back to interval, not
an approximate score. Solver caps remain 4,096 logical states, 65,536 work
units per call and 10,000,000 reserved work units per query. No exponential
branch enumeration is allowed beyond its declared cap.

The reference implementation intentionally keeps the graph, indexes, cores
and oracle in memory. Its job is exact decisions and logical work evidence,
not low physical memory. Account for construction memberships, dense summaries,
modal descriptors, posting occurrences, surviving body memberships, sparse
query preparation, admitted/refused solver work and output. Do not convert
heterogeneous record/cell/work counts to RAM bytes or elapsed milliseconds.

Validation steps: tests first; exact finite top-k/zero/self/tie/refusal cases;
reproduce every old G=4 union/interval body and posting counter; verify every
new result against direct set intersections and a posting-merge control;
retain per-query output, budget and input/code hashes; independent review
of the comparison and error boundaries. A small tighter-score count is not
a speedup. A reduction versus G=4 is not automatically the new theorem's
contribution: compare against interval at the same G.

## Completed Results

The retained [receipt](evidence/similarity-large-groups-20260921/receipt.json)
contains 900 checked complete outputs: two datasets, three group counts,
thirty fixed source queries and five methods. This is sixty distinct source
queries, not 900 independent samples. All ordered rows match a direct-set
oracle. The G=4 union/interval controls reproduce the old declared counters;
every posting control reproduces the old expanded membership count.

Each table row sums once over the thirty source queries. Members are the sizes
of surviving target bodies, not measured I/O reads or Python set operations.

| Dataset | G | Interval body members | Branch body members | Dual body members | Dual extra pruned blocks | Dual reduction vs same-G interval |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GrQc | 4 | 15,184 | 15,137 | 15,137 | 4 | 0.31% |
| GrQc | 16 | 12,273 | 11,997 | 11,997 | 17 | 2.25% |
| GrQc | 64 | 11,940 | 11,447 | 11,447 | 22 | 4.13% |
| Facebook | 4 | 236,399 | 236,227 | 236,227 | 4 | 0.07% |
| Facebook | 16 | 223,126 | 221,041 | 221,041 | 46 | 0.93% |
| Facebook | 64 | 206,686 | 202,422 | 198,213 | 102 | 4.10% |

At G=64, GrQc reads 1,086 modeled bodies instead of 1,130; Facebook reads
3,574 instead of 3,778. Eight GrQc queries and twenty-one Facebook queries
have at least one additional dual prune. The union-only counts are unchanged
across G: 20,622 members/1,566 bodies and 267,211 members/4,818 bodies.

The target-posting merge obtains exact overlaps without target-body reads.
It visits 7,260 expanded memberships for GrQc and 175,952 for Facebook at
every G. Consequently the body reductions above are not wins over DAAT.
The prior full-query negative timing results remain in force.

## Work Paid For Those Decisions

| Dataset | G | Branch admitted / budget refusals | Dual admitted / budget refusals | Tau refusals, each mode | Branch reserved work | Dual reserved work |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| GrQc | 4 | 351 / 0 | 351 / 0 | 44 | 6,601 | 6,626 |
| GrQc | 16 | 335 / 0 | 335 / 0 | 1 | 46,747 | 27,892 |
| GrQc | 64 | 321 / 1 | 322 / 0 | 0 | 382,260 | 48,410 |
| Facebook | 4 | 1,495 / 0 | 1,495 / 0 | 410 | 25,229 | 72,932 |
| Facebook | 16 | 1,610 / 3 | 1,613 / 0 | 155 | 1,444,693 | 291,680 |
| Facebook | 64 | 1,204 / 431 | 1,635 / 0 | 8 | 7,002,697 | 1,682,868 |

The dual avoids branch refusals, not the correctness requirement: refused
calls retain the safe interval bound. In Facebook G=64 this enables another
42 blocks/84 target bodies/4,209 members to be skipped compared with branch.
Both solvers evaluate the same exact envelope when admitted. The logical
state peaks at G=64 are 25 branch records versus 814 dual threshold cells
for GrQc, and 25 versus 3,654 for Facebook. These are different units.

The dual recurrence work equals its reservation here. Branch actual work at
G=64 is 380,792 for GrQc and 6,959,337 for Facebook. Reservations, recurrence
operations, row memberships and CPU time cannot be added as equal costs.
The per-query reservation budget does not cap all query work: refused dual
calls can already have prepared a core/plan; cheap filters, validation,
posting comparisons, heap operations and output are not fully metered.

At G=64 the shared resident preparation retains:

| Dataset | Dense capacity values | Dense population values | Modal core nodes | Owner runs | Deviating leaf records | Deviation keys |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GrQc | 332,867 | 167,744 | 16,702 | 13,374 | 23,039 | 23,039 |
| Facebook | 256,540 | 129,280 | 37,507 | 28,871 | 56,224 | 56,224 |

There is additionally one default slot per core node, source sets, both
posting indexes and oracle/output state. Records have different field counts
and Python overheads. Compile visits/signature counts are preserved in the
receipt; their summed temporary peaks are not a process peak. No byte-level
memory saving follows from this table.

## Attribution And Decision

Increasing G helps the existing interval control substantially before the new
exact envelope is used. Credit the exact bound only for the same-G increments.
The tau=8 G=4 Facebook result is slightly weaker than the old unclipped
automatic-overlap result (235,918 members/4,292 bodies); the present 236,227/
4,294 is a declared profile refusal effect, not a claim to reproduce that mode.

Most importantly, every modal call still follows dense O(G) laminar/interval
filtering and G-shaped histogram construction. The compressed kernel has not
removed that cost from the whole pipeline. Next derive an equally compressed
interval control and prove equivalence before studying a compressed-only
dispatch path. Do not run another timing variant of this dense-first driver
and advertise kernel counts as physical improvements.

Follow-up: the [compressed interval control](Similarity-Compressed-Interval-Control.md)
now proves and implements that equivalence, with a separate mathematical
review and 180 exact sparse-top-k parity outputs. Its new query view discards
dense summaries and histograms; the original driver and this receipt remain
unchanged. Exact-solver shared preparation, serialized storage and physical
evidence are still missing. The cheaper control receives the same reduction.

This is a modest positive decision-level opportunity and a concrete next
design constraint, not an end-to-end performance result, product RAM promise,
or proof of scientific priority.

## Verification And Review

The [independent review](Similarity-Large-Group-Workload-Review.md) found no
remaining concrete defect in the scoped driver and ran six tests plus sixty
additional independent bounded checks. Its accounting qualifications above
are retained. The review does not audit the entire research codebase or
establish novelty. The lead's fresh warnings-as-errors full similarity suite
passes 122 tests (23.029 seconds of test execution, not a benchmark).

An initial public run failed before writing a receipt because the driver
compared expanded postings with old `L` (compressed intervals). Source tracing
identified `membership_visits` as the correct field. Only the new driver was
repaired; the successful rerun was after a confirmed terminal failure. All
ten source-code hashes now match the retained receipt. No old kernel changed.

Receipt SHA-256:
`bf50ab091409266c153f82291b9001a2bcd0c1ec447a9d0faf074397f8d0796a`.

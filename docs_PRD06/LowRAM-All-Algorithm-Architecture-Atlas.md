# Low-RAM All-Algorithm Architecture Atlas

Status: active evidence synthesis

Product north star:
`docs_PRD04/A007-spc-founder-interview-prep-v7.md`

Frozen research corpus: `docs_PRD06/graph-learning/`

Audit evidence: `docs_PRD06/reference-learning/all-algorithm-lowram/evidence/`

## Executive Thesis

The product is not "Neo4j, rewritten in Rust." It is a bounded
analytical runtime. It accepts a graph-shaped job and calculates
complete working set, selects a legal execution plan, enforces a
hard RAM ceiling, and proves what happened afterward.

The architecture starts from the algorithm's access pattern and
mutable state, not one universal graph store. Every algorithm
receives a small portfolio of physical plans. The same logical
request may choose a fast resident plan, an exact spill plan, a
bounded approximation, or refusal. A plan is useful only if its
memory equation and correctness semantics can be tested.

```text
+------------------+
| Logical request  |
+------------------+
         |
         v
+------------------+     graph shape + B_ram + SLA
| Artifact planner | <-----------------------------
+------------------+
         |
         v
+------------------+
| Working-set test |
+------------------+
         |
         +-- fit
         +-- spill
         +-- approximate
         +-- refuse
         |
         v
               +---------------+
               | Budget guard  |
               +---------------+
                      |
                      v
               +---------------+
               | Result receipt|
               +---------------+
```

The public promise is stronger than a memory estimate:

```text
estimate + select + enforce + measure = workload contract
```

## How This Atlas Was Built

The corpus is frozen before interpretation. Three independent
reading lanes cover disjoint files with near-equal line budgets.
Each ledger binds its path to a SHA-256 hash and line count.
It records full read spans. Occurrences retain line spans.
Aliases
are merged only after the three ledgers are united.

```text
                     FROZEN CORPUS
                 88 files / 14,158 lines
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
        +-----------+ +-----------+ +-----------+
        | Lane 07   | | Lane 08   | | Lane 09   |
        | 4,715 ln  | | 4,736 ln  | | 4,707 ln  |
        +-----------+ +-----------+ +-----------+
              |             |             |
              v             v             v
        file ledger   file ledger   file ledger
        occurrence    occurrence    occurrence
        candidates    candidates    candidates
              |             |             |
              +-------------+-------------+
                            |
                            v
                  +-------------------+
                  | Alias resolution  |
                  +-------------------+
                            |
                            v
                  +-------------------+
                  | 3+ plans per algo |
                  +-------------------+
                            |
                            v
                  +-------------------+
                  | Executable audit  |
                  +-------------------+
```

This is the visible reasoning protocol used throughout:

1. Identify immutable inputs, mutable state, and output.
2. Identify sequential, random, and repeated access.
3. Calculate topology, state, scratch, I/O, and output bytes.
4. Decide which bytes must be resident.
   Tile, regenerate, compress, checkpoint, or spill the rest.
5. State exactness and determinism before discussing speed.
6. Derive a pre-run budget decision and a mid-run guard.
7. Specify an oracle and adversarial data that can falsify it.

## Common Cost Contract

The shared symbols are simple enough to appear in manifests and
test output:

| Symbol | Meaning |
|---|---|
| `n` | node, item, document, or vector count |
| `m` | edge, posting, or non-zero count |
| `d` | feature/vector dimension |
| `k` | requested neighbors, results, or clusters |
| `p` | active partitions, tiles, or worker lanes |
| `b_id` | bytes per stored identifier |
| `b_val` | bytes per score, weight, or state value |
| `B_ram` | user-declared process RAM ceiling |
| `B_os` | reserved OS/runtime safety margin |
| `B_topology` | resident topology or index pages |
| `B_state` | resident mutable algorithm state |
| `B_temp` | bounded scratch, queues, buffers, and merge state |
| `B_output` | resident output window; larger output must stream |
| `B_peak` | estimator's predicted process peak |

Every plan must instantiate this equation:

```text
B_peak = B_os + B_topology + B_state + B_temp + B_output

legal(plan) iff upper_confidence_bound(B_peak) <= B_ram
```

Disk capacity and bytes read are separate contracts:

```text
B_disk = artifacts + checkpoints + spill_runs + final_output

T_wall ~= T_compute + bytes_read / effective_bandwidth
                   + random_reads * effective_read_latency
                   + synchronization_and_merge_cost
```

An mmap file is not free RAM. The estimator includes the maximum
pinned or resident page window. Verification measures RSS,
resident mapped pages, allocator state, and kernel I/O buffers.

## Plan Portfolio

The plans are not quality levels on a single line. They expose
different contracts; a user may prefer any one of them.

```text
FAST FIT                         EXACT SPILL

+------------------+            +------------------+
| compact artifact |            | partitioned file |
+------------------+            +------------------+
| full hot state   |            | bounded window   |
+------------------+            +------------------+
| minimum passes   |            | merge/checkpoint |
+------------------+            +------------------+
 low latency                     more I/O, hard RAM


BOUNDED APPROX                    HYBRID

+------------------+            +------------------+
| sketch / sample  |            | hot exact tier   |
+------------------+            +------------------+
| explicit error   |            | cold spill tier  |
+------------------+            +------------------+
| fixed state      |            | adaptive planner |
+------------------+            +------------------+
 lowest RAM                      shape-sensitive
```

`refuse` is not an architecture option and does not count toward
the minimum three plans. Refusal is required when no plan can
honor the memory, correctness, output, disk, or deadline.

## Architecture Scoring Rules

Each retained plan is judged on six dimensions. A clever layout
without a trustworthy budget decision is demoted.

| Dimension | Required question |
|---|---|
| Budget | Can peak RAM be bounded before execution? |
| Correctness | Is it exact, epsilon-bounded, probabilistic, or heuristic? |
| Latency | What work and I/O replace resident memory? |
| Predictability | Which data-shape variables widen the estimate? |
| Verification | What oracle, metamorphism, or differential test breaks it? |
| Product fit | Does it strengthen A007's portable workload contract? |

```text
candidate
   |
   v
+-------------------+
| Equation complete?|-- no --> reject research claim
+-------------------+
   | yes
   v
+-------------------+
| Semantics explicit|-- no --> experiment only
+-------------------+
   | yes
   v
+-------------------+
| Oracle available? |-- no --> no production profile
+-------------------+
   | yes
   v
+-------------------+
| Guard enforceable?|-- no --> estimate, not contract
+-------------------+
   | yes
   v
+-------------------+
| Retain as profile |
+-------------------+
```

## Receipt Contract

Every successful or refused run emits enough information to
calibrate the estimator and reproduce the decision:

```text
receipt
+-- artifact hash and schema
+-- canonical algorithm and plan ID
+-- requested B_ram, deadline, and correctness mode
+-- predicted B_peak with confidence interval
+-- actual peak RSS and resident mapped pages
+-- bytes read, bytes written, and spill high-water mark
+-- wall time, CPU time, passes, and synchronization counts
+-- output cardinality, checksum, and error statistics
+-- estimator error and refusal reason
```

## Canonicalization Rules

The corpus mixes algorithms, physical data structures, protocols,
and test methods. They are related but not interchangeable. The
atlas uses this classification before assigning plan obligations:

```text
named corpus concept
        |
        v
+--------------------+
| Transforms state or|
| computes an answer?|
+--------------------+
        |
   +----+----+
   | yes     | no
   v         v
algorithm   +-------------------+
3+ plans    | Organizes bytes?  |
            +-------------------+
                   |
              +----+----+
              | yes     | no
              v         v
         structure   protocol or oracle
         crosswalk   crosswalk only
```

An algorithm family is canonicalized together only when its members
share all three of these properties:

1. The dominant topology access pattern is the same.
2. The asymptotically dominant mutable state has the same shape.
3. One physical plan can preserve each member's semantics with only
   parameter or operator substitutions.

This prevents two opposite mistakes. It does not create a fake engine
for every name in a glossary, and it does not hide materially different
memory behavior under a broad label such as "centrality."

Aliases retain all occurrence IDs. Supporting structures such as CSR,
Roaring bitmaps, FSTs, or record chains remain first-class evidence,
but they become reusable layout ingredients rather than standalone
compute sections unless the corpus also describes an update, build,
merge, or traversal algorithm over them.

The remainder is organized by canonical algorithm. Each section
contains at least three plans in the validator-enforced format.

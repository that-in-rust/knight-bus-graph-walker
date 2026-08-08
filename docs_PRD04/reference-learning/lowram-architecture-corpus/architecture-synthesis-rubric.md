# Architecture Synthesis Rubric

## Binding Product Test

An architecture is eligible only when it advances the A007 product: an immutable artifact-to-answer graph-analytics runner that admits work under a hard budget, executes a deterministic plan, and emits a proof-carrying receipt. Neo4j compatibility is an input/output adapter and oracle boundary. It is not permission to rebuild a transactional database.

## Shared Symbols

Every option SHALL express its working set using the subset of these symbols that materially applies:

| Symbol | Meaning |
|---|---|
| `n` | logical node count |
| `m` | logical directed edge count after projection |
| `m_u` | canonical oriented edge count for undirected processing |
| `p` | deterministic partition count |
| `t` | admitted worker count |
| `d` | embedding dimension |
| `k` | requested top-K or candidate-list width |
| `f` | maximum admitted frontier cardinality |
| `c` | current community count |
| `a` | active-node count in a delta iteration |
| `b_e` | bytes per stored endpoint or dense ID |
| `b_w` | bytes per edge weight |
| `b_s` | bytes per score/value |
| `b_o` | bytes per offset |
| `B_ram` | process memory ceiling |
| `B_os` | reserved OS/runtime/allocator headroom |
| `B_io` | admitted direct-I/O and decompression buffers |
| `B_out` | bounded result/output buffers |
| `B_tmp` | on-disk temporary-data ceiling |

The generic admission inequality is:

```text
upper_bound(
    B_os + B_topology_resident + B_properties_resident + B_algorithm_state
  + B_workers(t) + B_io + B_out + B_allocator_slack
) <= B_ram
```

Persistent artifact bytes and temporary disk bytes SHALL be reported separately from resident memory. Memory mapping does not make bytes free: the plan must charge the admitted resident-window/page-cache envelope and measure peak RSS/cgroup usage.

## Decision Vocabulary

| Decision | Meaning |
|---|---|
| `choose` | default retained design for at least one declared profile |
| `experiment` | credible retained alternative whose threshold needs benchmark calibration |
| `defer` | useful after the first product slice or after a prerequisite exists |
| `reject` | violates A007, has an unfavorable resource shape, or lacks a verifiable use case |

| Plan class | Required meaning |
|---|---|
| `fit` | exact execution whose complete upper-bound working set fits `B_ram` |
| `spill` | exact execution with bounded resident windows and deterministic temporary artifacts |
| `approximate` | explicitly opt-in error/recall/convergence contract with bounded state |
| `refuse` | no legal plan under the declared correctness, time, RAM, and disk contract |
| `hybrid` | deterministically mixes retained forms while preserving one receiptable contract |

## Option Scoring

Each retained option is scored qualitatively on eight independent axes. No single aggregate score can erase a hard failure.

1. **Correctness:** exact semantic equivalence or explicit approximation envelope.
2. **RAM:** complete upper bound, not just the central vector/queue.
3. **Latency:** cold and warm time-to-answer, including conversion and spill merge.
4. **Predictability:** sensitivity to degree skew, density, convergence, compression ratio, and worker count.
5. **I/O:** sequentiality, amplification, temporary bytes, and random-read count.
6. **Reuse:** value across named workflows without becoming a general database.
7. **Verification:** strength of Neo4j oracle, independent oracle, and metamorphic properties.
8. **Founder value:** relevance to security/IAM/dependency/SBOM/access-path users and willingness to pay for bounded execution/receipts.

Hard rejection conditions:

- requires unbounded queues, outputs, candidate pairs, or intermediate graphs;
- relies on the OS page cache without an admitted resident envelope;
- changes exact semantics silently when under memory pressure;
- imports WAL, MVCC, lock management, transactional recovery, or online mutation machinery without an A007 analytical need;
- cannot state a pre-run refusal condition;
- cannot be differentially or metamorphically verified;
- gains RAM only by omitting conversion, allocator, worker, output, or OS terms.

## Required Family Coverage

The final atlas SHALL include at least three alternatives for each of:

1. bounded paths and BFS;
2. WCC;
3. PageRank;
4. NodeSimilarity;
5. kNN;
6. Louvain;
7. Leiden;
8. triangle counting;
9. FastRP.

Every family SHALL retain at least one exact fit-capable and one exact spill-capable plan. Approximate options are additional, never an undeclared substitute for exact behavior.

## Required Verification Bundle

Every retained option SHALL define:

```text
small independent oracle
    + Neo4j/GDS differential fixture
    + metamorphic properties
    + adversarial graph shapes
    + deterministic replay
    + estimate-versus-measured calibration
    + forced-fit / forced-spill / forced-refuse tests
```

Adversarial fixtures SHALL include, where relevant: empty graph, isolates, self-loops, duplicate edges, cycles, diamond, star/high-degree hub, long chain, disconnected components, complete or near-complete graph, bipartite graph, equal-score/tie cases, weighted graph, and intentionally insufficient RAM/disk budgets.

## Selection Rule

The runtime selector may choose only from versioned options in the atlas. Given identical artifact identity, workload profile, resource contract, and engine version, selection SHALL be deterministic and receipt-visible. Calibration may alter future versioned thresholds; it SHALL not mutate an in-flight plan.


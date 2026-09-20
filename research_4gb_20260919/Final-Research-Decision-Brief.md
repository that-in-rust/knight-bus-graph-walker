# Bounded Graph Analytics Decision Brief

Date: 2026-09-20 local. Completed research synthesis; [coverage and acceptance](Source-Coverage-Completion-Audit.md) specify the full-read and supporting-evidence limits. No new production implementation, 4 GB benchmark, customer commitment or global novelty claim is made.

## The Recommendation

**Build a complete small-machine analytical workflow for one painful dependency-investigation job, with algorithm-specific storage behind a simple interface. Keep a separate structural-compute experiment for the unusually large gains. Do not make a complete Neo4j rewrite the prerequisite.**

The customer's promise is: bring a consistent dataset, choose a supported question and resource envelope, and obtain a correct useful answer before its decision deadline. Preparation, delivery and refresh belong to the promise. A low-RAM kernel that requires a large hidden builder, produces an unusable output, or misses the decision window does not fulfill it.

This recommendation applies a product lens associated with Shreyas Doshi and a systems lens associated with Jeff Dean. It is our judgment, not their endorsement. [A007](../docs_PRD04/A007-spc-founder-interview-prep-v7.md) supplies the founder-fit direction; the research does not establish a paying customer.

## The End-To-End Product

| Stage | Customer outcome | Critical responsibility |
| --- | --- | --- |
| Connect and interpret | Their CSV or Neo4j projection has the intended graph meaning | IDs, isolates, types, direction, properties, duplicates, source consistency and export impact |
| Select a plan | They understand the resource and waiting tradeoff | Bounded preview, supported parameter changes, honest build/query estimates and separate disk allowances |
| Prepare and publish | A usable, versioned representation exists | Bounded parser/sort/ID-map buffers, safe publication, restart and retained-file accounting |
| Ask and receive | A supported query yields useful original IDs and evidence | Admission, cold/warm latency, output completeness, backpressure and client memory |
| Refresh and repeat | The next answer is fresh enough without operational surprises | Changed-data correctness, old/new overlap, reader retention, fallback and source watermark |

The user should choose data meaning, question, machine budget, waiting tolerance and freshness. We should choose encodings, row layout, optional indexes and execution schedule. The full responsibility inventory and a concrete dependency-investigation walkthrough are in [End To End Workflow](End-To-End-Workflow.md).

Known query **families** can support different seeds, supported filters and parameters. A prepared adjacency layout is not a cached answer. A new seed can reuse the former; a completed PageRank vector generally cannot survive a changed projection or personalization unchanged.

Source data may change continuously while each computation uses a consistent snapshot. Snapshot-first is an initial product boundary, not a claim that live analytical algorithms are impossible. [Memgraph's online PageRank](https://memgraph.com/docs/advanced-algorithms/available-algorithms/pagerank_online) is a concrete existing approximate incremental example. Its availability does not prove exact parity or low memory for our workload.

## Resource Boundaries

Use decimal units. The target is a **4 GB physical machine**, with a provisional **3 GB worker allowance** and **1 GB OS/other reserve**, both requiring real measurement. The current storage scenario is **50 GB retained prepared blocks**, counted across the selected portfolio and pinned generations. Input bytes and peak temporary disk have separate limits.

This means:

- No unreported larger builder or source process on the same machine.
- No free page cache, FFI heap, diagnostic log, sort run or client-side result materialization.
- No automatic 50 GB allowance for every algorithm or every replacement generation.
- No claim that a container OOM kill is successful bounded execution.
- No guarantee of completed arbitrary exact queries merely because allocations are bounded.

Start with one heavy build/query/refresh stage at a time. Add concurrency only with a shared measured allocation model. A separate remote Neo4j source is outside the worker's RAM measurement but remains part of total system cost and extraction latency.

## Three Architecture Choices

| Path | Useful job | Mechanisms | Why RAM can fall | What can make it lose |
| --- | --- | --- | --- | --- |
| Snapshot investigation | Repeated dependency/reachability questions on a stable snapshot | Bounded builder, typed adjacency, physical read scheduling, bounded frontiers; later valid answer reuse | Keep topology and large mutable state on disk; avoid unnecessary properties and duplicate work | Cold dependent I/O, large frontiers/results, expensive refresh, insufficient query coverage |
| Structural computation | Analytics on graphs derived from memberships or genuinely repeated rows | Incidence-native operators, exact equivalence classes, feature-state PageRank | Avoid expanded edges and sometimes eliminate entity-sized changing vectors | Missing incidence input, wrong projection semantics, F approaching N, costly output or structure discovery |
| Budgeted batch | Scheduled rank, similarity or embedding jobs that currently exceed available RAM | Scan layouts, source/dimension batching, external aggregation and replay | Bound each active batch and externalize the rest | Many passes, dense candidate work, SSD writes, missed deadlines and expensive output |

The shared facilities are source semantics, versioning, bounded ownership, publication, output and measurement. The storage layouts and kernel schedules are deliberately different. A universal lowest-RAM/lowest-latency winner is not supported by the evidence.

## Where The Biggest Gains Could Come From

Prioritize reducing **work that need not exist** before merely moving required work to disk:

1. Do not expand a shared group into all pairwise edges when the requested operator can run on memberships.
2. Do not recompute an unchanged answer when a complete, sufficient validity certificate can establish reuse.
3. Do not propagate identical computation separately when the algorithm permits an exact quotient or factorization.
4. Improve locality and bound the lifetime of what remains.
5. Use external execution as a useful capacity option, with its latency and disk costs exposed.

The [seventeen-option decision map](Architecture-Decision-Map.md) distinguishes existing foundations from narrower contributions. External sorting, CSR, incidence graphs, residual methods, dynamic algorithms and factorized joins are established ideas. Many broad combinations also already appear in this repository. Their names are not inventions.

The strongest additional mathematical proposal is [D16 feature-state PageRank](Feature-State-PageRank.md): for the specifically defined shared-feature-count weighted graph, eliminate the entity rank unknowns, iterate matrix-free feature state, then reconstruct entity scores. This does not require identical entity rows. It does require the exact incidence-derived operator, and its stationary error contract is not automatically GDS's finite-iteration stopping behavior.

[D17 refresh certificates](Feature-Refresh-Certificates.md) then updates that feature residual under a fixed-universe, fixed-personalization membership-change contract. It includes the global dangling correction and affected-row closure. A passing certificate validates a new-version reconstruction; it does not mean all old scores remain unchanged. It is conditional research, not implemented live analytics.

Independent review found two lifecycle requirements now made explicit: preserve raw singleton membership provenance discarded by the analytical projection, and admit physical posting/score replacement even when the residual gate passes. A small logical update can still rewrite a large packed file. D17 now specifies bounded versioned overlays, a separate storage gate and explicit compaction/fallback lifetimes; these still need implementation tests.

## Quantities We Can Defend

| Evidence class | Quantity | Correct interpretation |
| --- | --- | --- |
| Prior recorded narrow benchmark | Warm Bolt p99: Knight Bus 3.9703 ms, Neo4j 5.3027 ms; sampled RSS 234.18 MB versus 374.05 MB | About 25.1% lower p99 and 37.4% lower sampled RSS in that small-output profile; historical artifacts inspected, experiment not rerun |
| Constructed incidence shape | 10M entities in 10k disjoint groups of 1,000: about 40.04 GB expanded topology versus 0.16008 GB two-way incidence topology | About 250x selected topology reduction, not a measured whole-Neo4j reduction |
| D16 selected state | 1B entity scores versus 1M feature values: two f64 vectors are 16 GB versus 16 MB | 1,000x selected iterative-vector reduction under the stated operator, not a 16 MB total application |
| D16 example work | 113 updates for the documented 1e-8 target move 2.26 TB of row payload | About 4,520 seconds of row service at an assumed 500 MB/s, before other work; not a latency measurement or one-hour guarantee |
| D17 selected state | Three f64 arrays at 1M features: 24 MB | Error accounting, counts, reverse indexes, changed-row discovery, full output and fallback are additional |
| D17 retained-storage example | Optional reverse postings lift the named portfolio to about 44.04 GB; retaining a new distinct 8 GB score plane would reach 52.04 GB | The failed-certificate fallback needs a different retention/output plan under a 50 GB cap |
| Build amortization example | 1,800 seconds of extra preparation and 55 seconds saved per eligible query | Time pays back after 33 queries on a valid reusable generation; not necessarily across changing daily snapshots |

Full shapes, versions, omitted costs and numerical assumptions are in the decision map and dedicated proofs. These figures must not be multiplied together or attached to arbitrary 50 GB inputs. The small exhaustive checks support restricted mathematical identities and catch counterexamples; they do not establish production memory bounds or scale performance.

## Verification-First Next Steps

1. **Obtain the actual job.** Ask for a recently painful dependency investigation or scheduled analysis: input/projection, existing query, desired output, deadline, freshness, current machine and workaround. Separate observed pain from a generic willingness to endorse lower RAM.
2. **Pin the answer.** Define direction, filters, duplicates, isolates, weights, ordering and output semantics. Reuse the existing narrow driver/oracle work only where compatible. Add small independent expected results and rejection cases before optimization.
3. **Prove the first full run.** Include export, validation, ID mapping, preparation, query and complete usable output under 4 GB. Exercise hubs, wide values, slow consumers, disk exhaustion, cancellation and mid-build crash. Measure peak live disk, not just final bytes.
4. **Prove reuse and the second run.** Change a seed, then change the data. Distinguish storage reuse, cached-answer reuse and recomputation. Test pinned old readers, stale results and correct fallback under the same resource contract.
5. **Select one differentiating fast path from measured structure.** Prefer incidence-native execution when input semantics make it available. Compare with a competent factor-aware baseline, not just an intentionally expanded graph. Retain a useful ordinary path when the structure is absent.
6. **Promote or stop on customer outcomes.** Report admitted completions, correct results, first-answer time, query percentiles, freshness, disk traffic, total cost and operator steps. Agree meaningful acceptance thresholds before timing. False answers, hidden budget overruns and consistently missed decision deadlines stop the proposal.

Do not require all seventeen mechanisms before delivery. Exact similarity and global iterative jobs should become separate follow-on tests, not claims inferred from a successful neighborhood walk. Keep D16/D17 as a focused structural experiment until a matching source and useful answer contract justify promotion.

## What We Know And Do Not Know

We have a concrete product workflow, a prior narrow compatibility/performance receipt, detailed source-derived constraints, multiple storage architectures, and restricted mathematical checks. We do not yet have a new measured source-to-answer 4 GB implementation, a general 50 GB latency quote, an exact complete Neo4j-compatible runtime, or verified buyer commitment.

The decisive experiment is **one user's first useful answer, another question on the prepared snapshot, and a changed-data refresh, all honestly accounted**. That is where the algorithm, storage format and product thesis meet.

# Packed Embedding Workflow Evidence

Date: 2026-09-20. V1 three-mode, V2 four-mode and V3 lossless compact-endpoint implementation/measurement completed. This is a restricted synthetic experiment, not a Neo4j comparison, a physical 4 GB demonstration or a novelty claim.

## Question

Does [compact mean-history replay](Embeddings-Mean-History-Replay.md) improve a useful source-to-complete-result frontier once a competent exact-sum replayer and a fused external-state implementation receive the same incidence, semantics and tolerance?

The earlier 17.5% packed-plane illustration was arithmetic, not a benchmark. It neither established physical RAM savings nor ruled out an external-state algorithm that uses less RAM and finishes faster. This experiment is designed to expose those outcomes rather than protect the proposed method.

## Matched Controls

| Mode | Retained RAM Planes | Additional Disk Work | Numerical Target |
|---|---|---|---|
| sum | Exact feature-local-width history | No mean conversion required | Same real operator, certified lattice approximation |
| mean | Compact histories plus transient exact sums | Write/read compact conversion files | Same real operator, two additional node-grid bits |
| disk | Current means plus next exact sums | Raw node state and normalized endpoint accumulation | Bit-identical rounded trajectory/output to mean at the same grid |

The disk baseline can fuse updating a node, normalizing it and aggregating the next feature sums. Its one raw node plane and output endpoint plane can be overwritten sequentially through independent cursors because each old row is consumed before replacement. This is a stronger comparator than an unnecessary double-sweep, two-node-plane baseline.

All modes emit the complete signed weighted combination of normalized layers, not just the last layer. Mean/disk must match output bytes. The independent oracle compares sum and mean separately against the same exact rational operator, because their different rounded trajectories need not be identical.

## Physical And Input Scope

- Host observed: Apple M4, 10 physical/logical cores, 25,769,803,776 physical bytes (24 GiB), macOS 15.3.1 arm64; compiler rustc 1.93.1. The executable is single-threaded, with no CPU affinity, thermal control or exclusive-host reservation.
- The input is canonical ordered dense-ID node-feature membership data, with exactly two distinct feature memberships per node. Counts and source validation are part of run preparation. Arbitrary Neo4j import, general edge-to-factor discovery and external-ID sorting are not tested by this contract.
- The synthetic initialization deliberately has a constant nonzero first coordinate. This isolates representation/execution costs from uncontrolled normalization refusals. It is not a real workload, a default SHA-profile acceptance rate, or a downstream embedding-quality result.
- Use fresh processes for time/RSS. `/usr/bin/time -l true` was executed as a telemetry sanity check; it reported a maximum resident set size and peak memory footprint. This command was not an algorithm benchmark.
- Logical bytes read/written are software-accounted traffic, not physical device traffic. Buffered I/O and the OS page cache can strongly affect timings; no forced cold-cache experiment is implied.
- RSS excludes some system-wide cache/kernel accounting. This host is not a demonstrated 4 GB physical machine. No result here can on its own prove the original whole-machine budget.

## Verification Plan

1. Compile the isolated Rust source with warnings denied and execute its focused tests.
2. Run the independent Python oracle on multiple small snapshots, dimensions and depths, for all three modes.
3. Compare the entire mean/disk files, not only a hash or selected rows.
4. Exercise invalid source and refused precision; a successful result must not appear.
5. Measure a feature-cardinality scenario with real cell-width savings and a small-feature/alignment scenario where those savings can disappear.
6. Record preparation, kernel, output, complete wall time, fresh-process RSS, live packed-plane bytes, full output bytes and peak local files. Include unsuccessful runs.

## Current Evidence

The independent Python oracle's initial five self-tests passed: known counter-hash values, signed exact normalization enclosures, a closed-form single-coordinate target, a complete golden output file and rejection of five corrupted-output variants. The mutations cover truncation, extra bytes, nonfinite output, wrong node ID and incorrect coordinates.

The lead compiled the completed V1 Rust source with warnings denied and overflow checks enabled; its 11 focused tests passed. Six small snapshots were then run through all three modes: 18 successful complete outputs, 1,512 independently checked coordinates, six whole-file mean/disk comparisons and six precision/truncation refusals. The same cases were subsequently checked against each receipt's actual claimed error bound, not merely the requested 1e-4 tolerance. All passed; these are the same cases, not 36 independent fixtures. Maximum observed oracle error upper bound was about 2.123e-8. The zero-depth single-coordinate case attains its reported bound, so a passing loose tolerance alone would have been weaker evidence.

The oracle independently expands the small source graph, propagates exact Fraction values and encloses each normalization with exact integer square-root inequalities at 96 output fractional bits. It validates binary headers, original IDs, complete length, finite values and every coordinate. This is independent of the Rust feature-replay implementation, but relies on the same declared mathematical target and source format.

The [blocked-history follow-on](Embeddings-Blocked-History-Frontier.md) derives a middle schedule between full replay and fused disk state. Its restricted cost planner passed 300 cases against 30,690 enumerated partitions. That is a separate logical result: it does not add an implemented mode or a runtime measurement to this experiment.

## V1 Measured Cases

### What Graphs Are Actually Being Tested

There is a useful mathematical interpretation of the two-membership source. Regard each feature as a vertex of a base multigraph and each source node as a uniquely identified edge joining its two features. The analytical graph's vertices are those source edges; two are adjacent when they share a base endpoint. Their weight is the sum of the weights of their shared endpoints. Thus this experiment studies a weighted line-graph-style projection, allowing parallel base edges, without materializing that projection.

This is an inference from the declared operator, not an observed customer workload. Events involving two entities could supply such a representation naturally, but an event embedding is not automatically an embedding of the original entities, and ignoring relationship direction can change the question. The experiment does not demonstrate discovery of a compact factorization of arbitrary Neo4j adjacency.

For feature cardinalities t_f, the total undirected edge weight of the projected graph is `sum_f w_f*t_f*(t_f-1)/2`. Its distinct unweighted edge count is `sum_f choose(t_f,2) - sum_{u<v} choose(m_uv,2)`, where m_uv counts source rows with that unordered membership pair. The subtraction accounts for pairs sharing both features. These identities explain potential projection expansion; no expanded graph-byte size is inferred from the source-byte column below. Measuring a real source's eligibility and downstream task quality is an explicit next research requirement.

All cases use seed 42, exact decimal TAU=0.0001, B=24 for sum and B=26 for mean/disk. All three target the same real ternary initialization and operator. The constant first coordinate deliberately supplies a norm floor; acceptance here is not an estimate of acceptance on general embeddings.

| Case | Nodes | Features | Dimensions | Transitions | Source Bytes | Complete Output Bytes |
|---|---:|---:|---:|---:|---:|---:|
| cardinality | 262,144 | 4,096 | 64 | 8 | 4,194,336 | 136,314,920 |
| alignment | 65,536 | 16,384 | 32 | 8 | 1,048,608 | 17,301,544 |
| depth | 32,768 | 512 | 16 | 24 | 524,320 | 4,456,488 |

Each mode ran in three fresh processes, rotating execution order. No cache eviction was attempted. Generated input was immediately available; temporary output and intermediate files could benefit from the OS cache. Source generation, post-run checksums and byte comparison are outside the engine time. Validation, prepared copying, propagation, normalization, complete output staging, result sync/rename and cleanup are inside it. The workflow does not promise directory-fsynced crash durability, nor does it flush all transient-file traffic to the physical device before stopping the timer.

### Time, Memory And Disk

MB and GB below are decimal. Times and OS memory columns are medians of three runs. Brackets give the observed minimum and maximum, not a confidence interval. Packed/file counts were identical across repeats. OS peak footprint is reported separately because it does not coincide with maximum RSS; neither statistic is a whole-machine memory bound.

| Case / Mode | Total Seconds [Range] | Max RSS MB [Range] | Peak Footprint MB | Packed Peak MB | Peak Owned Files MB | Logical Read+Write GB |
|---|---:|---:|---:|---:|---:|---:|
| cardinality / sum | 26.334 [25.946, 26.345] | 13.074 [13.042, 13.107] | 12.747 | 10.486 | 140.509 | 0.182453 |
| cardinality / mean | 27.009 [26.993, 27.022] | 15.663 [13.730, 15.663] | 15.287 | 8.651 | 140.509 | 0.199230 |
| cardinality / disk | 15.524 [15.491, 15.551] | 11.731 [10.158, 12.747] | 8.143 | 2.359 | 476.054 | 6.171919 |
| alignment / sum | 3.791 [3.777, 3.792] | 19.562 [19.464, 19.595] | 19.120 | 16.777 | 18.350 | 0.028836 |
| alignment / mean | 4.063 [4.050, 4.070] | 26.051 [25.887, 26.083] | 14.861 | 16.795 | 18.350 | 0.062391 |
| alignment / disk | 2.518 [2.517, 2.556] | 16.286 [16.056, 18.416] | 6.538 | 4.212 | 60.293 | 0.808977 |
| depth / sum | 6.628 [6.605, 6.632] | 3.686 [3.621, 3.686] | 3.294 | 0.983 | 4.981 | 0.018613 |
| depth / mean | 6.776 [6.762, 6.785] | 3.146 [3.146, 3.178] | 2.835 | 0.795 | 4.981 | 0.020186 |
| depth / disk | 2.287 [2.284, 2.287] | 3.391 [3.342, 3.572] | 3.015 | 0.074 | 15.467 | 0.542377 |

The owned-file peak excludes the supplied source, filesystem allocation slack and unrelated files. Add retained source bytes for a source-plus-worker local-file accounting, and account for any other simultaneous jobs separately. The experiment does not include source export, arbitrary ID mapping, factor discovery, refresh generations or a retained query portfolio.

### Phase Medians

| Case / Mode | Preparation s | Propagation s | Output s | Conversion s |
|---|---:|---:|---:|---:|
| cardinality / sum | 0.003065 | 16.438905 | 9.862851 | 0 |
| cardinality / mean | 0.003060 | 17.041452 | 9.965533 | 0.063546 |
| cardinality / disk | 0.002874 | 15.316094 | 0.196169 | 0.061440 |
| alignment / sum | 0.000819 | 2.115098 | 1.665294 | 0 |
| alignment / mean | 0.000828 | 2.355771 | 1.696433 | 0.126435 |
| alignment / disk | 0.000864 | 2.485862 | 0.026052 | 0.124864 |
| depth / sum | 0.000464 | 4.901458 | 1.719947 | 0 |
| depth / mean | 0.000482 | 5.053088 | 1.717243 | 0.012249 |
| depth / disk | 0.000475 | 2.271819 | 0.006944 | 0.032274 |

Conversion is a subset of propagation, not an additional serial phase. Disk propagation includes initialization and normalization; replay output includes reconstructing and normalizing every layer. The total also includes setup/publication outside these named phases, and medians of separate columns need not sum to the median total. The disk result's short output phase does not mean normalization was omitted.

## What Changed Because Of Measurement

1. **Reject the direct payload-to-RAM inference.** In cardinality, ordinary mean replay saved 17.5% packed bytes but used 19.8% more median maximum RSS than sum replay. It was 2.6% slower. Alignment made packed payload slightly larger (0.107%), median RSS 33.2% larger and time 7.2% longer. In depth, mean used 14.7% less median RSS but was 2.2% slower. These results are specific to the observed metric and host; the footprint column must not be concealed when it differs.
2. **Keep the strong disk baseline.** It was 42.5%, 38.0% and 66.3% faster than mean replay in the three cases. Its logical traffic was about 6.172, 0.809 and 0.542 GB, versus 0.199, 0.062 and 0.020 GB for mean. Small kernel state and linear-depth propagation can win even while moving many more bytes. This does not predict cold-device performance.
3. **Preserve low-I/O replay as an option, not the latency winner.** It avoids the raw/endpoint disk planes and repeated state traffic. A storage-write constraint or slow external device may favor it, but those conditions have not been measured here.
4. **Remove an implementation-imposed detour.** The [arena compaction revision](Embeddings-Arena-History-Compaction.md) proposes the same codewords in one paid allocation with forward conversion in place. The width-order proof removes the need for conversion files; actual time and RSS remain to be measured. Generic factor-aware competitors may use the same optimization.
5. **Do not stop at three friendly cases.** This is evidence about a declared canonical source and conditioned initialization. It does not establish production eligibility, downstream quality, GDS equivalence, a 50 GB source result, a deterministic deadline or paper-worthy novelty.

## Reproduction And Receipts

- [V1 complete verification](experiments/packed-verification-v1.jsonl): requested-tolerance oracle, full equality and refusal receipts.
- [V1 claimed-bound verification](experiments/packed-claimed-bound-v1.jsonl): the same 18 outputs checked against each claimed error bound.
- [V1 fresh-process measurements](experiments/packed-benchmarks-v1.jsonl): all 27 raw receipts, unabridged `/usr/bin/time -l` output, source/result hashes and environment.
- [V1 engine archive](experiments/packed-engine-v1.tar.gz) and [V1 driver/oracle archive](experiments/packed-driver-v1.tar.gz): exact original experiment sources. Archived engine SHA-256 is `924700e69325df1a4388f29dea58aa93da882351be6823a160c45fd78c56e2bd`, verified from the archive and matching the measurement manifest. The driver/oracle are subsequently extended; use these archives for the original versions.
- [Current runner](experiments/run_embedding_packed_evidence.py) and [independent oracle](experiments/verify_embedding_packed_results.py): exclusively create new receipt files, never overwrite previous evidence; remove only their own generated temporary datasets/results on completion.

The original compilation used `rustc --edition=2021 -O -Dwarnings -C overflow-checks=yes`, with `--test` added for the test binary. From a directory containing the archived engine, driver and oracle:

```sh
rustc --edition=2021 -O -Dwarnings -C overflow-checks=yes embedding_packed_workflow.rs -o /tmp/kb-packed-v1
python3.11 run_embedding_packed_evidence.py verify --binary /tmp/kb-packed-v1 --receipts verification-new.jsonl
python3.11 run_embedding_packed_evidence.py bench --binary /tmp/kb-packed-v1 --receipts benchmarks-new.jsonl --repeats 3
```

An early archive-hash command failed because the host's Perl `shasum` process could not initialize `C.UTF-8`; repeating the archive extraction/hash with `LC_ALL=C` succeeded. This was a tooling locale failure, not a failed engine test or corrupted archive.

## V2 Arena Results

The four-mode common executable completed 36 fresh-process runs: three cases, four modes and three repeats, with 18 complete mean/disk and mean/arena file comparisons. All completed. [V2 raw receipts](experiments/packed-benchmarks-v2.jsonl) retain every sample, not only the medians below. [V2 source archive](experiments/packed-workflow-v2.tar.gz) preserves its exact engine, runner and oracle. The seven-case independent correctness run passed 28 outputs / 2,352 coordinates against claimed bounds, 14 complete-file comparisons and eight refusals, including a valid source with unused feature columns.

| Case / Mode | Median Total s | Median Max RSS MB | Median Peak Footprint MB | Packed Peak MB | Logical Read+Write GB |
|---|---:|---:|---:|---:|---:|
| cardinality / sum | 25.394 | 13.009 | 12.649 | 10.486 | 0.182453 |
| cardinality / mean | 26.175 | 15.925 | 13.927 | 8.651 | 0.199230 |
| cardinality / disk | 15.353 | 11.731 | 8.897 | 2.359 | 6.171919 |
| cardinality / arena | 24.824 | 11.223 | 10.863 | 8.651 | 0.182453 |
| alignment / sum | 3.678 | 19.530 | 19.137 | 16.777 | 0.028836 |
| alignment / mean | 3.966 | 24.052 | 14.861 | 16.795 | 0.062391 |
| alignment / disk | 2.506 | 16.089 | 6.947 | 4.212 | 0.808977 |
| alignment / arena | 3.732 | 19.644 | 19.235 | 16.795 | 0.028836 |
| depth / sum | 6.384 | 3.621 | 3.212 | 0.983 | 0.018613 |
| depth / mean | 6.570 | 3.146 | 2.753 | 0.795 | 0.020186 |
| depth / disk | 2.267 | 3.457 | 3.048 | 0.074 | 0.542377 |
| depth / arena | 6.211 | 2.998 | 2.622 | 0.795 | 0.018613 |

Arena conversion-file reads and writes were exactly zero in every measured run. Its complete output and peak owned-file sizes equal mean/sum on these inputs. No sum/mean/disk arithmetic was changed; a shared read abstraction was added, so timings are compared within this common V2 build rather than treating cross-build timing changes as arena gains.

- Cardinality: arena versus sum used 13.7% less median RSS and took 2.2% less median total time. Versus ordinary mean it used 29.5% less RSS and took 5.2% less time. The absolute arena/sum RSS difference is only about 1.79 MB in this experiment.
- Alignment: arena versus sum used essentially the same RSS (0.6% higher median) and took 1.5% longer. In-place conversion removes the file detour but cannot manufacture a packed-width advantage.
- Depth: arena versus sum used 17.2% less median RSS and took 2.7% less time; the absolute RSS difference is about 0.62 MB. Versus ordinary mean it used 4.7% less RSS and took 5.5% less time.
- Disk remained 38.2%, 32.8% and 63.5% faster than arena across the three scenarios. Its much higher traffic and temporary disk demand still matter; arena is not the universal winner.

These are small repeated observations with uncontrolled host activity, not statistically established population effects or deterministic latency guarantees. Arena time ranges were 24.777-24.840 s, 3.723-3.741 s and 6.210-6.226 s. Its RSS ranges were 11.190-11.223 MB, 19.415-19.677 MB and 2.900-3.031 MB. All raw control ranges remain in the linked receipts.

### Precision Selection Is Not Yet Optimized

B=24/26 was chosen to give the mean route a no-worse worst-case raw error reservation, not to minimize memory at TAU=1e-4. Actual reported bounds often have substantial slack. A smaller admitted B can cross another byte-width boundary for either control. Therefore these tables do not establish the minimum RAM achievable by any mode at the common tolerance.

A useful next comparison should choose precision under a charged planning procedure. For this deliberately constant-first-coordinate source, that coordinate remains exactly M at every layer, so all row norms are at least M. Worst-case division/mean defects and the output normalizer's width bound can consequently give a conservative pre-run precision recipe. That property does not hold for arbitrary initialization. Observed admission need not be assumed monotone in precision without proof; repeated full runs also have a cost. The lossless disk6 comparison does not depend on changing B and should hold it fixed.

The next isolated comparison held B fixed and changed only the endpoint representation and its codec. Its completed results follow.

## V3 Lossless Endpoint Results

[C6 endpoint recoding](Embeddings-Center-Certificate-Storage.md) is implemented as `disk6`. The same common executable ran `disk` and `disk6` on the three cases above, with three repeats each: 18 successful runs, nine complete-file comparisons, nine exact publication-bound comparisons and nine exact accounting comparisons. [Raw V3 receipts](experiments/packed-benchmarks-v3.jsonl) end with a successful completion record. [The V3 source archive](experiments/packed-workflow-v3.tar.gz) preserves engine, runner, oracle and algebraic probe. The benchmark deliberately does not rerun the independent rational oracle on these large cases; the separate seven-source verification checked 14 complete outputs / 1,176 coordinates and four refusals.

Times and RSS below are medians [minimum, maximum] from the three repeats. Decimal units and the same host/cache/accounting qualifications above apply. The modes use the same initialization, B=26, precision gate and eager in-place disk pass schedule.

| Case / Mode | Total Seconds [Range] | Max RSS MB [Range] | Median Peak Footprint MB | Peak Owned Files MB | Logical Read+Write GB |
|---|---:|---:|---:|---:|---:|
| cardinality / disk | 15.672 [15.568, 15.691] | 10.191 [10.125, 13.009] | 7.111 | 476.054 | 6.171919 |
| cardinality / disk6 | 16.947 [16.857, 17.013] | 10.420 [9.994, 10.682] | 8.438 | 308.281 | 3.152020 |
| alignment / disk | 2.519 [2.516, 2.556] | 18.383 [15.991, 20.087] | 6.849 | 60.293 | 0.808977 |
| alignment / disk6 | 2.695 [2.688, 2.713] | 20.513 [18.579, 20.546] | 7.045 | 39.322 | 0.431489 |
| depth / disk | 2.274 [2.272, 2.282] | 3.441 [3.408, 3.457] | 3.064 | 15.467 | 0.542377 |
| depth / disk6 | 2.397 [2.394, 2.417] | 3.408 [3.375, 3.555] | 3.048 | 10.224 | 0.280233 |

| Case | Logical Traffic Change | Peak Owned-File Change | Median Time Change | Median Max-RSS Change |
|---|---:|---:|---:|---:|
| cardinality | -48.93% | -35.24% | +8.14% | +2.25% |
| alignment | -46.66% | -34.78% | +6.98% | +11.59% |
| depth | -48.33% | -33.90% | +5.43% | -0.95% |

The live packed-plane reservation was exactly unchanged: 2,359,296, 4,212,192 and 73,728 bytes respectively. Complete results and retained prepared/result files were also unchanged. C6 saves temporary endpoint state and repeated logical traffic, not the size of the final result or retained source copy. The differing RSS/footprint observations do not establish an OS allocation mechanism or a stable population effect.

Median user CPU seconds increased from 15.05 to 16.56, 2.42 to 2.64, and 2.19 to 2.35; median system CPU seconds decreased from 0.44 to 0.25, 0.06 to 0.03, and 0.03 to 0.02. This is consistent with a codec-versus-buffered-I/O tradeoff, but it is not a causal attribution to one instruction or physical device throughput. The compact and legacy codecs also differ in record handling; an equalized legacy record-call control has not been implemented.

**Measured conclusion:** C6 reduced logical traffic by 47-49% and peak owned files by 34-35%, while making all three cached workloads slower by 5-8%. Keep it as a disk-budget option, not a measured latency or RAM improvement. A cold/limited-storage workload might change the time ranking, but this experiment has not established that regime. No additional test rerun is needed merely to restate these completed results.

## Decision Rule

A useful outcome may be a measured win, a restricted frontier, or a negative result. If the fused disk baseline matches accuracy with lower RAM and lower complete time in the tested regime, the mean-history method has not earned preference there. If mean compression only saves a few packed bytes while process RSS does not change, report that directly. Search for a defensible workload or revise the algorithm; do not relabel payload counts as customer savings.

The seven-family research goal remains open regardless of this single experiment's outcome.

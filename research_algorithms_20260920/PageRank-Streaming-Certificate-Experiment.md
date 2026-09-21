# Scalar Publication Certificate: Implementation And Controls

Date: 2026-09-21. A01 research experiment. This tests the certificate/publication stage, **not a new complete PageRank solver or a Neo4j comparison**. The [derivation](PageRank-Streaming-Publication-Certificate.md) and [independent mathematical review](PageRank-Streaming-Certificate-Review.md) define the contract and numerical precedent.

## What Changed

The separate [implementation](experiments/streaming_rank_publication_certificate.py) holds one supplied binary64 factor state h and one binary64 scatter vector. High-precision values are scalar row enclosures and aggregate error ledgers. It publishes only after the computed bound passes, preserves existing output on refusal, and has no automatic higher-memory fallback. The frozen native and mass probes remain unchanged.

The source manifest is newly validated, not trusted merely because its bytes hash correctly. One row pass counts exact factor cardinalities and writes isolate flags; another checks every exact degree against the cardinalities. The builder retains 8F cardinality payload bytes and uses the existing full-row decoder. It writes a 72-byte header, N isolate-flag bytes and 8F cardinality bytes. A full source-hash scan and metadata-hash scan are charged. This is not a bounded external importer and does not make complete-job physical RAM promises.

For each query, verify source and metadata hashes. Uniform effective personalization is enclosed directly as `3/(20N-17*isolates)`; a point source uses its indexed isolate flag. These formulas avoid repeatedly accumulating rounded uniform probabilities. Enclose signed row sums with downward/upward Decimal contexts; denominators are the exact integers `20*d+17*q`. Decode the actual packed output value for Epub before writing the same byte buffer and updating its hash.

The implementation rejects nonfinite h, negative or subnormal nonzero scatter values, scatter/output overflow, mismatched identity, invalid dimensions/personalization and insufficient selected-array budget. The gamma variant requires each factor size below 2^52. The stronger published Rump variant uses exact binary exponent extraction and directed scalar arithmetic, without that artificial gamma count cutoff. Neither variant stores a Decimal vector.

The prototype assumes source, trusted manifest and exclusively owned h remain immutable during the call. Path hashes do not establish a concurrent snapshot. Atomic replacement protects the prior score file on failed admission; the returned certificate is not persisted transactionally with that file, and fsync/readback durability is not claimed.

## TDD And Review

Five initial tests failed because the module was absent, then passed after implementation. The enlarged suite checks 160 source fixtures: all 128 incidence families on three vertices, plus 32 fixed-seed cases with four through seven vertices. Three personalizations and two bound variants yield 960 complete outputs. Exact expanded-graph Fraction solutions bound-check the actual output bits, while the unchanged direct original-operator certifier independently checks the gamma outputs; the stronger variant must emit identical bytes.

Separate cases cover six arbitrary factor states, including signed h with admitted nonnegative scatter; repeated groups; isolate-only answers; malformed degrees; changed source/metadata; nonfinite states; preservation of prior output; no leftover temporary files; selected-state budget refusal; binary exponent scales; summation sharpness; and the stricter-tolerance case where the published ufp bound admits identical exact bytes that gamma refuses. These are finite tests, not a formal proof or a process-memory measurement.

Two comparison-driver tests first failed on the missing driver. They now exercise four complete three-query publication paths, requiring accepted post-timing direct audits and identical gamma/Rump/direct-same output hashes; another test rejects tampered factor-state files.

Independent mathematical review supports the declared proof, adds exact failure witnesses, and identifies Rump's stronger same-state summation bound. It did not inspect code. The completed [implementation audit](PageRank-Streaming-Certificate-Code-Review.md) found five concrete defects. Each was reproduced by a failing regression before the lead's fixes:

| Finding | Implemented correction | Verification boundary |
| --- | --- | --- |
| `sys.float_info.rounds` did not detect changed runtime mode | Runtime binary64 half-ulp operations reject directed modes instead of trusting a startup field | Isolated Darwin arm64 upward-mode regression passes and restores the environment; arithmetic mode must stay fixed during the call |
| Direct output could replace source/metadata | Reject resolved input/output path aliases before work | Source, metadata and symlink aliases tested in both direct modes |
| Worker exceptions erased earlier observations | Structured failed-worker outcomes; atomic receipt checkpoints after every triplet; query/audit failures retained | Fault-injected fourth worker preserves the first complete triplet and records failure with no ratio; sentinel test times are not performance data |
| Empty/shortened workload was accepted | Enforce three ordered sources and complete state descriptors, then validate every state file before publication | Empty, shortened and reversed workload regressions pass |
| Post-commit filesystem reporting could raise | Read file sizes and construct substantive receipt before replacing the answer; no source/metadata stat after commit | Injected post-commit-stat failure no longer triggers for either publisher |

The encoder-discrepancy regression additionally confirms that altered serialized bytes are charged to Epub and refused while preserving the old answer. At the timing freeze, the scalar-certificate suite has ten passing tests (0.592 seconds) and the comparison suite six (0.043 seconds). These include the 960 exact complete-output comparisons described above. The frozen base probe was not changed. The reviewers were read and closed, and all tests exited before timing. The lead verified the fixes with regressions; no second independent code-audit claim is made.

## Frozen Comparison Plan

Use the same separately licensed MovieLens-derived prepared incidence file: N=1,682, F=943, Z=100,000, no isolates. Its source provenance and license restrictions remain in [the native study](PageRank-Native-Incidence-Evidence.md). Raw source, metadata, factor-state files and individual answer vectors remain outside the repo.

First, the unchanged factor-CG solver produces certified scores for the same three personalizations at tolerance 1e-12. A separately charged source/score scan maps each answer into a shared factor state `h=B^T D^-1 x`. This changes the candidate reconstruction and is **not** a free extraction of an existing solver state. Save exact state-file hashes. The benchmark compares publication from these identical supplied states at final tolerance 1e-10; it cannot measure whether a redesigned solver would reach them at lower cost.

| Mode | Reconstruction | Certificate | Purpose |
| --- | --- | --- | --- |
| `direct` | Existing floating reconstruction | Frozen actual-file two-pass interval residual | Strongest current inexpensive reconstruction/control |
| `direct_same` | Same scalar row enclosure and emitted bytes as stream | Frozen actual-file two-pass interval residual | Separates certificate choice from output rounding |
| `stream` | Scalar row enclosure | Scalar ledger with conservative gamma bound | Original proposal, not the tightest known bound |
| `stream_rump` | Identical bytes to stream | Scalar ledger with published ufp bound | Stronger same-state numerical control |

Every mode pays the same source/metadata identity policy and state loading. Each worker publishes three complete answers. Capture wall/CPU time and maximum worker RSS BEFORE a separate direct original-output audit of all three files. That independent audit is excluded from stage time and reported separately for every mode, including the direct control. Also retain the later post-audit worker RSS: the validation process actually runs those extra Decimal arrays, so pre-audit RSS is a phase metric, not the complete worker's final maximum.

Run fresh serial `direct / candidate / direct` triplets, with each of the three candidate modes repeated three times and order rotated: nine triplets, 27 workers and 81 outputs. The already declared 15% endpoint-consistency screen controls which candidate/control time ratios may be reported. All raw results remain; there is no retry-until-favorable rule. The paired ratio uses complete worker session time, including state loading; per-publication phase time is also retained. Three passing pairs are not a tail-latency confidence study.

Builder and upstream solve/state-mapping time/RSS are reported separately. They are not silently subtracted from an end-to-end comparison: this experiment simply does not make that comparison. Logical reads distinguish source validation, publication, direct certificate, identity hashes, state loading and optional audit. Tiny repeated header reads and runtime/OS cache traffic are not all represented by those payload counters.

## Results

The [completed receipt](PageRank-Streaming-Publication-Results.json) contains nine triplets, 27 fresh workers and 81 accepted complete outputs, each additionally accepted by the post-timing original-output audit. All nine triplets pass the predeclared control-consistency screen; none was repeated. Within each method/query, output hashes and certificate bounds are repeat-stable. Gamma, Rump and `direct_same` emit exactly the same complete score bytes for every query. The ordinary direct reconstruction emits different rounding but also meets the same requested L1 tolerance.

**Finding: the scalar publication path removes the Decimal vectors and about half the modeled publisher read traffic, but is approximately 13% slower than the strongest current direct path on this source. Its worker-RSS range overlaps the control.** It is faster than the same-row-enclosure direct control, but that control does extra reconstruction work that the ordinary direct path does not require. Do not use that weaker comparison as the performance headline.

| Mode | Median local time ratio to direct | Passing ratio range | Median three-query session, ms | Pre-audit worker RSS, MB | Post-audit worker RSS, MB |
| --- | ---: | --- | ---: | --- | --- |
| Direct, 18 control workers | 1.000 | Reference | 169.919 | 19.562-20.529 | 19.562-20.529 |
| Stream, gamma | 1.135 | 1.133-1.136 | 193.009 | 19.431-20.349 | 19.710-20.611 |
| Stream, published Rump bound | 1.132 | 1.127-1.145 | 193.411 | 19.333-19.907 | 19.661-20.169 |
| Direct, identical enclosure/bytes | 1.845 | 1.825-1.873 | 314.901 | 20.201-20.431 | 20.234-20.513 |

MB is decimal. Each candidate has three locally consistent pairs. Ratios are medians of local ratios, not ratios of the displayed absolute medians. Ordinary direct control sessions range from 167.875 to 174.277 ms; all wall/CPU ratios are close to one. This is a small cached local experiment, not a confidence interval, hardware-isolated tail-latency study or whole-system resource guarantee.

Across the three queries, modeled publisher reads are 5,191,592 bytes for either direct mode and 2,571,824 bytes for either stream mode, a 50.46% reduction. Both also load/hash 45,264 factor-state bytes at session level. The modeled publisher counts include source/metadata identity scans, but exclude small repeated headers, preparation JSON, filesystem/cache behavior and the separately reported post-timing audit. Publication writes are the same 40,368 score bytes. One publication/source scan replaces reconstruction plus two source/output certificate scans; source identity verification still adds a full source read.

### Bound Tightness Is A Separate Metric

These bounds apply to the identical output bytes from gamma, Rump and `direct_same`:

| Query | Gamma L1 upper bound | Rump L1 upper bound | Direct actual-file upper bound |
| --- | ---: | ---: | ---: |
| Uniform | 1.958e-13 | 1.406e-13 | 1.166e-15 |
| Point source 0 | 1.832e-13 | 1.311e-13 | 6.103e-16 |
| Point source 1681 | 1.953e-13 | 1.479e-13 | 5.184e-15 |

All are below the requested 1e-10. The Rump control tightens the gamma bound without changing output or buffer count, but both scalar-ledger bounds remain much looser than direct certification here. The conservative scatter term dominates: roughly 2.28e-14 to 2.52e-14 before resolvent amplification for Rump, while weighted factor residuals are roughly 2.15e-16 to 8.66e-16. These are certificate bounds, not measured actual errors on the public source. Tiny exact-oracle tests, not this table, establish actual-error comparisons.

### Preparation And Scope

The trusted manifest is 9,298 bytes. Its builder takes 13.645 ms in the recorded preparation, retains 7,544 cardinality payload bytes, and performs two row-validation scans plus source/metadata hashing. Upstream CG, score-to-factor mapping and manifest preparation together take 632.319 ms; preparer maximum RSS is 20.496 MB. The three source/score-to-h mapping phases cost 7.611, 7.720 and 7.681 ms, in addition to the upstream solves. All are retained in the receipt rather than treated as free.

The query's selected h/scatter payload is 15,088 bytes and its retained Decimal-vector count is zero. Python runtime, row decoding, scalar arithmetic, allocation behavior and source buffers remain. During independent post-timing auditing, the streaming worker subsequently does allocate reference interval vectors; its post-audit RSS is explicitly shown. Consequently neither the selected payload nor the pre-audit range is a complete physical-memory budget.

### Reproduction And Integrity

Both reviews and all tests were terminal before this run. Driver session 6357 completed with exit 0. Current code hashes match the receipt; the frozen original base probe still has its prior hash. Use fresh external directories and a new receipt path for reproduction, keeping separately licensed raw data and score/state artifacts outside Git:

```sh
/Users/amuldotexe/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 \
  research_algorithms_20260920/experiments/benchmark_rank_publication_comparison.py \
  --store /tmp/knight-bus-ml100k-rows.bin \
  --folder /tmp/knight-bus-publication-reproduction \
  --receipt /tmp/knight-bus-publication-reproduction.json \
  --repeats 3
```

| Artifact | SHA256 |
| --- | --- |
| Scalar certificate | `022f3e8cea9ca870d7042bd2497818954705c85d71c93a27e5177e20d7de807a` |
| Comparison driver | `b6e02850c85b7bc691a68e4c0eae05fe351c694c9b504cce6e5b5cc582a0a2af` |
| Scalar tests | `d926d0aadd0227380d3cf67abde1b9eb548aa69b093c10172df5747c2d975dc8` |
| Driver tests | `c861846d8de06f0cf8a136aa4b719458ff6f2e71cc50c7a510127d191c718af5` |
| Public receipt | `a3cddba9e6cff3b9da18a3055cd098bea119ca6ef5ba06db6e77998cbee98e90` |

The experiment is on macOS 15.3.1 arm64, bundled Python 3.12.14. This publisher experiment does not use a dense library or NumPy. The raw source and retained prepared-source hashes remain those in the native experiment; the receipt binds the exact generated factor states and certificate metadata as well.

## Decision Rule

A useful positive result must preserve complete output/error guarantees and improve a paid resource dimension beyond removing arrays on paper. Retain any slower time, looser bound, refusal or unchanged RSS. Rump's numerical improvement is published precedent, not a novel graph result. Even a positive stage result leaves source validation/import, solver integration, bounded fallback, real large-source relevance and distinct scientific contribution to establish across A01 and the other six families.

The Decimal reference is also not a minimum-memory implementation of direct residual certification. A two-buffer binary64 outward-interval implementation would have 16F endpoint payload bytes; releasing the consumed h after reconstruction could make its phase maximum smaller than retaining h alongside the endpoints. The proposed publisher itself retains 16F h/scatter payload bytes. Therefore its credible remaining structural advantage could be pass count or a different accuracy/work frontier, not an asymptotic state improvement over every direct certifier. That binary64 implementation needs its own rounding proof, admission tests and measurements before it can be used as a measured comparator. No existing benchmark here establishes optimality.

The next falsifiable change is where precision is spent: replace per-membership Decimal row summation with an explicitly bounded binary64 row sum, leaving only per-row scalar enclosure arithmetic. The [derivation's next-step section](PageRank-Streaming-Publication-Certificate.md#10-next-precision-placement-test) describes that possible schedule and its extra rounding obligation. It is not implemented in the frozen results above. If it fails against equally optimized binary64 interval/direct controls, retain that failure rather than promote a pass-count saving as a speed/RAM result.

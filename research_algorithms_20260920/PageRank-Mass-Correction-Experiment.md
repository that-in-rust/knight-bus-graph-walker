# Mass Correction: Paid Sessions Against Cheaper Controls

Date: 2026-09-20; paired results integrated 2026-09-21. A01 implementation and experiment. This extends the [native-incidence study](PageRank-Native-Incidence-Evidence.md) without changing its source code or eight-mode receipt. The proposed projection is ordinary coarse correction specialized to a native graph source, not a newly invented linear solver.

**Result:** mass correction removes the observed reduced-power iteration penalty, but only matches ordinary power's elapsed time on this source. Established factor CG and reused dense solving remain faster. No non-dense process-RAM saving is established. The next useful target is the certification lifecycle, not another claim that fewer coordinates necessarily run faster.

## Premise And Evidence Boundary

Reduced power was 4.81x slower than fused ordinary power on the previous three-query aggregate. Its late uniform-query factor residual has one sign, so the [reviewed identity](PageRank-Mass-Correction-Review.md) proves the ideal candidate's L1 error equals its mass deficit. This does not prove that the spatial error is a pure scaling of the answer, or that there is exactly one spectral mode to eliminate.

The [mass-correction derivation](PageRank-Mass-Conservation-Correction.md) proposes preserving one exact global equation via a source-derived coarse vector. The independent review supports correctness and convergence, but identifies cheaper normalization and relaxation controls. This experiment includes them before making a practical claim.

## Implementation

The separate [probe](experiments/probe_mass_projected_pagerank.py) imports the unchanged native builder, row reader and original-output Decimal certifier. Its [tests](experiments/test_mass_projected_pagerank.py) reuse the independently expanded Fraction oracle, not a factor solver as the truth source.

### Streamed Static Correction

The builder stores one 16-byte `(u64 factor size, f64 coefficient)` record per factor, plus a 64-byte header identifying the source hash and coarse denominator. Coefficients use the positive formula `(C s)_f=(1-a)*sum_{i in f} d_i/(d_i+a*q_i)`. Damping is fixed to the original exact 17/20 contract. The builder retains two F arrays and is not an external-memory importer.

The solver retains only old/new changing F buffers. It does not retain the static coefficient arrays. Initialization gathers h, computes the personalization-dependent scalar G and analytically handles isolates. Two factor-record scans project the initial h. Each raw update uses one entity-row scan, followed by a metadata scan for the unprojected residual and coarse dot product; a second metadata scan corrects next only if continuing. Thus K checks use `2K+1` factor-record scans when F>0. Empty factor sets bypass the denominator.

The metadata is bound to the complete prepared-source hash, checked before query iteration. This extra identity scan is charged. Atomic output replacement occurs only after the provisional file passes the original-operator interval certificate. This is not a durable/fsync or recovery benchmark.

### Required Cheap Controls

The relaxed control uses ordinary interval-optimal Richardson: if `lambda(I-C)` lies in `[0,beta]`, set `omega=2/(2-beta)` and update `hNew=omega*T(h)+(1-omega)*h`. Its exact C-norm contraction bound is `beta/(2-beta)`, at most a under the native q<=d contract. The code derives beta from the actual maximum q/d in initialization; the displayed floating beta is a numerical parameter, not a directed-rounding theorem certificate. Original raw residual and final-file checks remain mandatory.

The normalized control uses ordinary reduced iterates and checks a normalized candidate at raw checks 1,8,16,... or when the raw screen already passes. It accumulates active candidate mass mu during the ordinary row pass. At those fixed checkpoints, one additional entity pass screens

```text
c = target_active_mass / mu
R_normalized = c * a B (T(h)-h) - (c-1) bEffective.
```

If the screen is small, publication scales active scores by c, leaves analytic isolate scores unchanged, and runs the same independent interval certificate on those actual bytes. This is not a free final fix: its screen scans, output and certificate are charged. It does not renormalize every iteration or assume a scalar mass correction fixes all spatial error. The checkpoint schedule is fixed before timing and is not asserted optimal.

A projected-relaxed mode combines the two exact mechanisms. It checks the RAW T(h)-h first, then projects the relaxed state. The two metadata scans still suffice. It may take more steps than unrelaxed projection even with a better worst-case contraction bound.

### Reused Dense Control

At F<=1024, assemble C and its Cholesky factor once per three-query session. All three personalizations then share the factorization. Each query pays for RHS assembly, triangular solves, full reconstruction and the original certificate. This removes the previous experiment's per-query factorization cost without hiding its initial build or quadratic resident storage. Reject larger F before NumPy import/matrix allocation. F=0 uses the isolated-graph answer without any factorization.

## Frozen Session Protocol

Use the same separately licensed MovieLens source and integer-degree projection as the previous experiment: 1,682 vertices, 943 factors, 100,000 memberships. All raw data, prepared files, metadata and individual rank files remain outside the repo; only aggregate receipts are retained. Attribution and conditions remain at the [original GroupLens README](https://files.grouplens.org/datasets/movielens/ml-100k-README.txt). No demographic files are used.

The three queries are uniform p, unit p at zero-based movie 0 and unit p at movie N-1. Exact damping is 17/20 and final certified L1 error must not exceed 1e-10. Screening uses epsilon/8 and is not itself a rigorous certificate.

| Mode | Session reuse / declared work |
| --- | --- |
| fused | Original one-pass power, same prepared factor source |
| filepower | Same recurrence with alternating vertex-score files |
| reduced | Previous unprojected factor-only power, retained negative control |
| cg | Previous ordinary factor-space CG |
| vertexcg | Previous ordinary two-pass degree-scaled CG |
| projected | Build static correction once, stream it during all queries |
| dense_reuse | Assemble and factor C once for all three queries |
| relaxed | Ordinary optimally relaxed reduced power, no correction file |
| normalized | Ordinary reduced power with declared final-normalization checkpoints |
| projected_relaxed | Same paid correction metadata plus optimal constant relaxation |

Run three repeats of each mode, rotating mode order by repeat. Each repeat is a fresh worker that completes all three queries: 30 workers and 90 final outputs. Read/close the independent reviewer and finish tests before any public timing. Run workers sequentially, setting BLAS/OpenMP thread-cap environment variables to one; these settings do not verify host isolation or every library's actual thread count.

Report per-session setup, each query's preparation/solve/output/certification, total session time, child wall time and peak worker RSS. Source ingestion is a common separate fresh-worker cost. Report cold-first-query cost as setup plus the first query and multiquery cost as the complete session, not the median of individual-query times with setup omitted.

Logical counters distinguish row scans, correction-record scans, changing-score files, dense pair updates, identity checks and output. They are algorithmic payload counters, not physical disk traffic or every repeated header read. Source/metadata generation is immutable for each worker. Parent/coordinator RAM and system cache are outside worker RSS; no physical 4 GB guarantee follows.

## Pre-Timing Verification

Five tests first failed on the absent module; the first implementation passed. A sixth test then failed on missing cheap controls before those controls were added. The final oracle suite covers 128 complete three-vertex incidence families plus 96 fixed-seed larger cases. For each of two personalizations, four new solvers produce complete answers: 1,792 original-Fraction comparisons. Separate cases add nine reused-dense outputs, 27 cheap-control outputs, exact projection/energy identities on 45 rational states, and metadata-binding, truncation, budget and preallocation refusal checks.

That enlarged suite exposed a real defect in the new normalized control: repeatedly adding uniform probabilities could make an all-isolate source appear to have tiny positive active mass. The implementation now derives isolate personalization mass from an exact integer count (or exact source-isolate flag), then divides once. A retained seven-isolate regression first failed before this fix. This corrects all three new preparation paths; the original base probe remains unchanged.

The independent reviewer used a distinct set of 24 fixtures and 96 signed rational states to challenge the proof, and supplied a counterexample where treating projected update difference as raw residual understates true error. It did not inspect implementation code. The implementation retains the raw residual and final original-operator certificate. No independent code-audit claim is made.

## Results

The initial [30-worker receipt](PageRank-Mass-Correction-Results.json) contains 90 accepted complete outputs with repeat-stable work counts. It also exposes a timing nonstationarity: unchanged fused sessions take 3,458.366, 740.068 and 728.454 ms; projected takes 3,437.087, 3,436.395 and 719.187 ms. A median-of-methods comparison would falsely suggest a large projected penalty because the fast regime begins midway through the second repetition. The cause was not established. These raw medians are NOT used to rank latency. The receipt is retained as correctness/work evidence, not discarded or overwritten.

### Paired Follow-Up Protocol

The independent [paired driver](experiments/benchmark_mass_pagerank_sandwich.py) wraps the UNCHANGED solver code. Each candidate session is immediately preceded and followed by a fresh fused-control session on the same three queries. Repeat this triplet three times for each of the nine non-fused candidates, rotating their order: 27 triplets, 81 workers, 243 final outputs. All method-specific setup remains charged inside the candidate session; common source ingestion is not remeasured.

Before this follow-up runs, define a local-consistency screen: `max(controlBefore,controlAfter)/min(...)<=1.15`. For passing triplets, report `candidate/sqrt(controlBefore*controlAfter)`; retain failing triplets with raw measurements and no accepted ratio. Retain ALL outcomes, without an automatic retry-until-favorable loop. This criterion examines controls only, not whether the candidate wins. It does not prove that hardware conditions were constant inside the candidate or that the host was isolated.

The wrapper also records worker process CPU time alongside wall time. CPU time can help diagnose scheduling effects but is not immune to frequency changes and is not substituted silently for wall time. Report ratios and spread descriptively; three triplets are not a tail-latency or population confidence study.

Two paired-driver tests pass: one validates the predeclared screen including invalid inputs, and one executes a complete tiny control/candidate/control triplet, requiring nine certified outputs and positive CPU measurements. They run only after the initial public process is terminal. The algorithm code, its hash, and the initial receipt remain unchanged.

### Completed Paired Results

The [paired receipt](PageRank-Mass-Paired-Results.json) contains all 27 triplets, 81 workers and 243 accepted complete outputs. All certificates bound actual published-score L1 error by at most 1e-10. Current source hashes match the recorded driver and both probes. Work counters and output hashes are identical across repeats of each method/personalization; different methods need not emit identical bits. Of 27 triplets, 24 pass the predeclared endpoint screen. No triplet was retried.

Lower time ratio is better; 1.000 means the geometric mean of the immediately adjacent fused-control session times. The reported median uses only passing triplets. It is a descriptive local ratio, not a confidence interval or an extrapolation to other graphs. The worker RSS range includes all three candidate executions, including locally unstable timing pairs, and uses decimal MB.

| Method | Passing pairs | Median time / fused | Passing ratio range | Checks: uniform / first / last | Worker RSS, MB |
| --- | ---: | ---: | --- | --- | --- |
| Fused ordinary power | Reference | 1.000 | Reference | 18 / 17 / 18 | 18.858-20.251 across 54 controls |
| File-backed ordinary power | 3/3 | 0.995 | 0.967-1.025 | 18 / 17 / 18 | 19.612-19.677 |
| Uncorrected reduced power | 3/3 | 4.833 | 4.745-5.594 | 108 / 116 / 116 | 19.808-20.398 |
| Ordinary factor CG | 3/3 | 0.781 | 0.774-0.783 | 11 / 11 / 11 | 18.858-19.956 |
| Ordinary vertex CG | 2/3 | 0.955 | 0.924-0.986 | 10 / 10 / 10 | 19.546-20.300 |
| Streamed mass correction | 2/3 | 1.010 | 0.997-1.024 | 18 / 17 / 18 | 19.366-19.759 |
| Reused dense factorization | 3/3 | 0.451 | 0.436-0.464 | Direct solve | 55.263-55.558 |
| Reduced relaxation | 3/3 | 3.797 | 3.720-3.861 | 78 / 86 / 94 | 19.808-19.923 |
| Final-normalization checkpoints | 3/3 | 5.557 | 5.553-5.638 | 108 / 120 / 120 | 19.546-20.136 |
| Mass correction plus relaxation | 2/3 | 3.815 | 3.800-3.831 | 78 / 86 / 94 | 19.808-19.857 |

All times include three queries, method-specific setup, full output and the same original-operator certificate. Common ingestion, coordinator RAM, system page cache and unmeasured refresh are not included. The source has only 1,682 vertices and 943 factors; these are cached local Python experiments, not billion-edge Rust or Neo4j/GDS measurements.

The three excluded comparisons are retained explicitly:

| Method / zero-based repeat | Before / candidate / after, ms | Control spread |
| --- | --- | ---: |
| Projected-relaxed / 0 | 1,251.751 / 13,036.079 / 3,477.553 | 2.778 |
| Vertex CG / 2 | 3,419.103 / 3,101.218 / 1,986.201 | 1.721 |
| Projected / 2 | 1,426.557 / 1,340.506 / 710.408 | 2.008 |

Even fused-control session times range from 702.978 to 3,552.217 ms across the complete experiment. Most workers' wall/CPU ratios are close to one, so scheduler waiting alone does not explain that change. CPU frequency, thermal state and competing work were not measured; the cause remains unknown. Endpoint pairing reduces one visible confound, but cannot establish constant speed during the candidate. Consequently, do not interpret the 1% projected/power difference or 0.5% filepower/power difference as a reliable performance distinction.

### Resource Interpretation

Projection reduces the total check count from 340 to 53, an 84.41% reduction against uncorrected reduced power. It pays for a 15,152-byte metadata artifact, one builder row scan plus a source-hash scan, per-query source identity verification, and 37/35/37 factor-record scans. That is a useful repair of the particular iteration, not a win over ordinary power's 53 checks.

The two changing projected buffers hold 15,088 payload bytes. This number does not include the row decoder, runtime, metadata-builder arrays, output buffers or certifier. The unchanged certifier retains 1,886 Decimal interval endpoints and makes two complete source/output scans per query. Releasing a vertex-sized iterate therefore does not establish a correspondingly smaller worker, much less a bounded whole-machine job.

CG's median paired ratio corresponds to 21.88% less session time than its adjacent power controls; reused dense corresponds to 54.93% less. These percentages are not direct CG-versus-projected ratios from matched triplets. Dense retains a 7,113,992-byte F-by-F matrix payload and performs 16,807,190 pair updates once per session, plus factorization/workspace. Its roughly 55 MB worker RSS is materially above the roughly 19-20 MB non-dense workers. Reuse is real and paid, but quadratic storage makes this unsuitable as an unqualified large-F recipe.

The relaxed control has a better worst-case contraction bound than uncorrected reduced power, yet its projected variant is much slower than unrelaxed projection here. A worst-case bound does not determine the populated spectrum or useful convergence for these three personalizations. Final normalization also fails to remove the remaining spatial error early enough to help under its fixed checkpoint schedule. These are retained negative findings, not reasons to remove strong controls.

### Verification And Reproduction

The combined native/mass algorithm suite passed 15 tests in 42.854 seconds after the isolate fix, before public timing. The separate paired-driver suite passed two tests in 0.128 seconds before the paired run. No solver code changed after those runs. The completed independent mathematical review is [here](PageRank-Mass-Correction-Review.md); it is not an implementation audit.

With the separately licensed prepared source available outside the repo, reproduce to a NEW receipt path so that this evidence remains immutable:

```sh
/Users/amuldotexe/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 \
  research_algorithms_20260920/experiments/benchmark_mass_pagerank_sandwich.py \
  --store /tmp/knight-bus-ml100k-rows.bin \
  --receipt /tmp/knight-bus-mass-paired-reproduction.json \
  --repeats 3
```

| Artifact | SHA256 |
| --- | --- |
| Mass probe | `b01a08731384960341c43de1e9e771bf82ea6d61758847c457f2de1bbea71c64` |
| Mass tests | `eb7ebcf66f00d77778d042004ef6a99d316538bf699b3b852128d9db10a70991` |
| Paired driver | `dc73f3e49d54d52e037cc9476a76b1333688cb4ac94318039486e59f108da2ce` |
| Paired tests | `6740d54a852671c388f7a0af98ec323b1122023e903eea3b92e0a2396992becb` |
| Initial nonstationary receipt | `b18363531973094f7d64d695c6e3f5660c6d5f14d82a3990315f69158df2351c` |
| Paired receipt | `f649ae6830b95771b26f1c44aef3f01daf5f990edeb66aee3340abdb91355d5b` |

The paired receipt also records the frozen base-probe and prepared-source hashes. Individual data and output files were kept outside the repo; complete-output hashes and interval certificates are retained in the aggregate receipt.

## Decision Gates

1. Does projection materially reduce the observed reduced-power work once its builder and metadata scans are charged?
2. Is that result already matched or beaten by ordinary relaxation or final-only normalization?
3. Does it beat CG or amortized dense solves on a meaningful time/RAM/disk dimension?
4. Is any measured difference larger than runtime/certificate overhead, and can it plausibly survive a useful source-size regime?
5. Is there a scientific contribution beyond an ordinary application of coarse correction? A speedup by itself does not establish that claim or the seven-family goal.

### Decision After This Experiment

Gate 1: yes, the check reduction survives charging metadata in these sessions. Gate 2: the implemented relaxation and final-normalization schedules do not match unrelaxed projection's time here. Gate 3: no overall superiority over CG or reused dense is demonstrated, and file-backed power is another equally fast low-resident-state control. Gate 4: no measurable non-dense worker-RSS saving is established. Gate 5: novel scientific contribution remains open; ordinary deflation is not renamed as an invention.

The follow-on [streaming publication certificate](PageRank-Streaming-Publication-Certificate.md) derives a bound using two f64 factor arrays and scalar directed accumulators instead of two Decimal factor arrays. Its later [implementation and controlled stage experiment](PageRank-Streaming-Certificate-Experiment.md) are now complete: 81 independently audited outputs, about half the modeled publisher reads, but about 13% higher time than the strongest direct path and no established whole-process RAM win. That study uses identical supplied states derived from ordinary factor CG; it does not claim superiority for this projected solver or alter the frozen mass-correction measurements above.

# PageRank: Precision Placement And A Stronger Direct Control

Date: 2026-09-21. Status: implemented, independently reviewed candidate; 43 focused tests and a completed 81-output public-source experiment. This continues A01 of the seven-family research goal. It is not a claim of seven completed innovations, a new summation theorem, or a Neo4j performance result.

## 1. The Question That Survived The Previous Experiment

The [frozen scalar-publication study](PageRank-Streaming-Certificate-Experiment.md) removed high-precision factor vectors and reduced modeled publisher reads by 50.46%, yet ran about 13% slower than the ordinary direct publisher. Its RSS range overlapped the reference. The next question is therefore not whether the same schedule can acquire a more impressive name. It is whether a different placement of numerical precision produces a useful complete-output resource/accuracy tradeoff.

The prior scalar path makes directed Decimal additions for every membership. On the existing natural source there are 100,000 memberships but only 1,682 vertices. Move ordinary binary64 addition into the membership loop, retain high-precision scalar work at the row and final-ledger boundaries, and charge the resulting uncertainty. The ratio Z/N motivates this test; it does not predict a 59-fold speedup.

Three lenses govern the comparison: numerical validity of the emitted answer, actual cost of the publication workflow, and a skeptical comparison against an equally low-memory direct certifier. A fourth, product-level lens asks whether any stage saving survives import, solving, refresh, output and the requested tolerance. The experiment below addresses only the publication stage, not that full product question.

## 2. Candidate And Controls

| Path | Membership arithmetic | Certificate state | Source/output schedule | Main risk |
| --- | --- | --- | --- | --- |
| Frozen direct | Ordinary floating reconstruction, Decimal residual endpoints | Two F-sized Decimal endpoint lists; supplied h remains alive | Reconstruct, then two actual-output residual scans | Expensive precision and retained objects |
| Frozen scalar/Rump | Decimal row enclosures; binary64 scatter | h plus one F-sized binary64 scatter | One publication scan and a factor-cardinality pass | Per-membership precision cost; conservative bound |
| Precision-placed scalar/Rump | Signed and absolute sequential binary64 row sums; Decimal only at row/ledger boundaries | Same h/scatter payload as frozen scalar | Same pass count as frozen scalar | Wider row enclosure; changed output bits; possible refusal |
| Binary64 direct control | Ordinary reconstruction; outward binary64 residual endpoints | h consumed before two F-sized double endpoints | Same full passes as direct | Conservative intervals and extra outward-rounding calls |

The fourth row is an ordinary stronger control, not our claimed invention. Its phased selected factor payload can be 16F bytes, the same as the candidate. It rules out the easy but incorrect argument that removing Decimal arrays alone establishes a general memory separation from direct certification.

The candidate is implemented in [precision_placed_rank_certificate.py](experiments/precision_placed_rank_certificate.py). It loads a private instance of the frozen publisher and changes only the row enclosure, leaving its source validation, residual ledger, same-buffer output accounting and publication/refusal mechanics intact. The normally imported frozen module is not mutated. Both the new file and all reused dependencies must be hashed in a timing receipt.

## 3. Inherited Mathematics And Our Schedule

This remains the restricted native binary-incidence model, not an arbitrary weighted directed graph database. Let B be the vertex/factor incidence matrix, q the membership count per vertex, Q=diag(q), and A=BB^T-Q. Exact integer degrees are d_i=sum_(f containing i)(s_f-1). Retained factors have size at least two; isolated vertices remain in the output universe. Damping is exactly 17/20. Uniform and single-vertex personalization are supported. See the [original derivation](PageRank-Streaming-Publication-Certificate.md) for the reduced system and treatment of dangling vertices.

For fixed finite binary64 h, interpreted as exact dyadic data, one active row needs

```text
H_i = sum_(f containing i) h_f
y_i = (20*b_i + 17*H_i) / (20*d_i + 17*q_i)
X_i = d_i*y_i
```

The new implementation computes Hhat_i and Ahat_i, sequential binary64 sums of h_f and abs(h_f) in exactly the same order. With u=2^-53, the published bound gives an error radius

```text
EH_i = (q_i - 1) * u * ufp(Ahat_i)
H_i belongs to [Hhat_i - EH_i, Hhat_i + EH_i]
```

The source is [Rump, Error Estimation of Floating-Point Summation and Dot Product](https://www.tuhh.de/ti3/paper/rump/altRu11.pdf), Theorem 3.5. The paper allows IEEE underflow and assumes no overflow. The theorem, its same-order absolute-sum premise, and its lack of a summand-count restriction are inherited numerical analysis, not new graph theory. The implementation evaluates the radius with directed scalar Decimal operations, not an unrestricted application of the paper's separate floating-evaluation corollary. Single-entry and empty sums are exact. Binary `frexp`/`ldexp` extracts ufp, including subnormals.

Outward scalar arithmetic then encloses the displayed rational row expression. The denominator is formed as a Python exact integer before conversion to Decimal, avoiding a falsely exact binary64 degree above 2^53. The frozen publisher chooses an admitted nonnegative normal-or-zero yhat, scatters it, and records the weighted row enclosure distance. Its original identity Bs=d+q allows a single scalar row ledger rather than an F-sized vector of row errors.

```text
membership loop          row boundary             final boundary
----------------         ----------------         ----------------
signed binary64 sum --->  outward row interval --> weighted Erow
absolute binary64 sum    rounded emitted score    weighted residual
                         exact-byte Epub           scatter-error bound
                                                      |
                                                      v
                                            complete-answer L1 gate
```

The retained publication bound is

```text
Efinal = (17/3) * (Rhat + Escatter + Erow) + Epub.
```

Changing how the row interval is obtained does not erase Erow or Epub. In particular, serializing a rounded approximation and certifying an ideal unrounded reconstruction would be incorrect.

## 4. Arithmetic And Ownership Admission

The public candidate checks actual runtime nearest/even operations, then probes gradual underflow with encoded subnormal inputs, a subnormal output, and signed cancellation near the normal/subnormal boundary. FTZ or DAZ behavior is not admitted. This is a supported-environment check, not a proof that another thread or native library cannot change arithmetic behavior during the call. The mode must remain fixed.

Nonfinite h, overflow in either row sum, scatter overflow and inadmissible reconstructed scatter values are refused. The candidate retains the frozen publisher's stricter normal-or-zero scatter rule even though signed row-sum evaluation itself permits subnormals. A negative lower row endpoint can therefore lead to a conservative refusal even when an ideal answer is nonnegative. No hidden higher-RAM fallback is performed.

All methods require an immutable, already semantically validated source plus the trusted manifest. Hashes alone are not semantic validation and do not prevent concurrent mutation. The manifest builder still retains an F-sized cardinality array and decodes complete rows. No bounded external importer or arbitrary skew-independent row-memory bound is added here.

The binary64 direct wrapper is explicitly ownership-consuming: after reconstructing the candidate file it empties the caller-supplied h array before requesting endpoint arrays. Numerical refusal still consumes h. This is disclosed and tested; a caller wanting retry without rereading h must pay for another retained copy. The selected payload maximum does not imply that the Python allocator returns physical pages to the OS.

## 5. What The Initial Exact Tests Establish

Fourteen tests first failed on the missing new module, then passed in 0.531 seconds. The inherited complete-output test checks 160 graph fixtures times three personalizations times two scatter bounds: 960 outputs against exact expanded Fraction PageRank answers. The new row tests add 40 signed-summation cases, including cancellation, smallest subnormal values, very large exponents and deterministic random rows, plus 16 rational-row enclosures with degrees up to 2^64-1.

Other inherited checks cover signed h, duplicates, isolates, malformed input identity/degrees, insufficient selected payload, gamma/Rump differences, output preservation, altered serialization and avoiding post-commit source-stat operations. A new counter test requires zero per-membership high-precision operations and no mutation of the normally imported frozen module. A new fault test requires the public entry point to run the stronger underflow admission.

The completed [independent review](PageRank-Precision-Placement-Review.md) found no soundness defect under the contract, but exposed two regression gaps. The inherited changed-mode child tests the frozen module, and the no-mutation assertion originally captured its reference after import. Both gaps are now covered by additional tests: snapshot all shared module bindings before loading the candidate, and exercise the actual new public entry point in an isolated child under upward, downward, toward-zero and live ARM64 FZ modes. The FZ test observes flushed result bits before requiring refusal and preservation of an existing answer. No x86 FTZ-only/DAZ-only execution is claimed.

The reviewer also showed that the original randomized row cases mostly had exact sums. An added 150-case mixed-exponent/mantissa test requires over 120 cases to exhibit real summation error and checks containment at precision 20, 50 and 100. A separate exact-answer witness requires the new publisher to refuse when the frozen path accepts, preserving the old output. These additions change tests, not the reviewed implementation hash.

The final lead-run pre-timing command executed 43 tests in 1.377 seconds with zero skips: 17 precision-placement tests, 17 independently authored binary64-control tests and nine comparison/integration tests. The control has separately expanded Fraction residuals as well as exact stationary-answer tests. The integration checks include complete outputs and audits, direct/binary-direct identical bytes, consuming h before endpoint allocation, failed-output preservation, malformed workloads and recorded worker failure. Synthetic one-millisecond driver fixtures are not performance evidence. Passing tests does not establish novelty or a universal correctness proof.

## 6. Predeclared Comparison

Reuse the frozen MovieLens-derived source and the exact three factor-state files from the prior experiment. Reusing those assets preserves a common input; it does not make their historical preparation cost disappear. Their hashes, dimensions, trusted manifest and original preparation receipt are recorded again. Raw data, state files and per-query answers stay outside the repository under their existing licensing restrictions.

Compare `precision`, `binary_direct` and `stream_rump` in three rotated rounds. Each candidate is bracketed by a fresh `direct` worker. Every worker must produce three complete answers and pass an independent direct actual-output audit after its stage timing and pre-audit RSS measurement. Record post-audit RSS separately. Nine triplets mean 27 workers and 81 outputs if all finish. The existing 15% control-consistency screen excludes unstable time ratios, not their raw observations. No retries to obtain a favorable result.

The [new driver](experiments/benchmark_precision_publication_comparison.py) reuses private copies of the frozen driver for state/workload validation and audit. It records identity checks, state load, publication, source passes, logical bytes and complete outputs. Comparison metrics include tolerance acceptance, bound tightness, output hash differences, local time ratios, worker RSS, and selected payload. Actual final-answer error is known only on tiny exact-oracle tests, not inferred from a public-data certificate upper bound.

The previous study's frozen ordinary direct path is a competent reconstruction plus certificate, but not necessarily the final strongest numerical implementation. If the new ordinary binary64 control wins, that is a valid narrowing of the opportunity. The candidate cannot claim general RAM superiority merely by outperforming the Decimal reference.

## 7. Rubber-Duck Checks And Decision Rules

| Claim to challenge | Correction or decisive test |
| --- | --- |
| No per-edge Decimal work means no per-edge work | Two ordinary additions per membership remain, as do decoding and scatter |
| O(N) precision implies O(N) runtime | Total work remains O(Z+N+F), with potentially large constants |
| The row theorem assumes h is nonnegative | It uses a same-order absolute sum and supports signed h; the later scatter has a separate positivity contract |
| Narrower CPU arithmetic is always adequate | Added enclosure width can cause refusal or demand more solver work |
| Fewer passes guarantee faster execution | Cached CPU cost can dominate; test rather than infer |
| 16F bytes establishes a physical memory ceiling | Interpreter, row state, source buffers, other phases and OS effects remain |
| A direct certifier needs Decimal vectors | Outward binary64 endpoints are a stronger ordinary control |
| Same input h means identical answers | Different row enclosures change rounding; compare actual output hashes and tolerance |
| A successful publisher makes the solver faster | Upstream solve and state mapping are unchanged and separately charged |
| This is a new summation result | Rump supplies the bound; any contribution must concern useful graph-source integration and its demonstrated tradeoff |

The positive gate is a reproducible useful tradeoff against the strongest full-output control, with honest precision/admission costs. A faster publication phase alone would still leave full lifecycle RAM, larger native sources, solver integration, nearest-art positioning and the other six family contributions open. A negative result is retained and changes the next research action; it is not rebranded as a success because the code passes tests.

## 8. Executed Public-Source Results

The [receipt](PageRank-Precision-Publication-Results.json) contains all nine planned triplets, 27 fresh workers and 81 accepted complete outputs. Every answer also passes the separately timed frozen actual-output audit. All nine pairs pass the predeclared control-only consistency screen; no repetition was discarded or retried. Worker session 68338 exited 0. Both agents were closed and the complete focused test command was terminal before timing.

**The precision-placement candidate reduces locally paired publication-session time by 62.12% against the frozen direct path, an approximately 2.64x speedup for this three-query stage. The ordinary binary64 control itself reduces that reference time by 25.18%. No convincing physical-RAM saving is established.**

| Mode | Median paired time ratio to frozen direct | Stable paired-ratio range | Median three-query session, ms | Pre-audit worker RSS, MB | Post-audit worker RSS, MB |
| --- | ---: | --- | ---: | --- | --- |
| Frozen direct, 18 workers | 1.000 | Reference | 167.859 | 19.530-20.152 | 19.530-20.169 |
| Precision-placed scalar, 3 workers | 0.379 | 0.378-0.388 | 64.141 | 19.300-19.808 | 19.546-20.070 |
| Binary64 direct, 3 workers | 0.748 | 0.745-0.765 | 127.876 | 19.284-20.529 | 19.677-20.775 |
| Frozen scalar/Rump, 3 workers | 1.137 | 1.135-1.151 | 192.069 | 19.284-19.677 | 19.612-20.038 |

MB is decimal. The ratios are medians of local candidate/geometric-control ratios, not ratios of the displayed absolute medians. There are only three candidate observations per mode, not a confidence interval or a p99 study. Frozen direct sessions range from 166.317 to 174.817 ms. Wall/CPU ratios range from approximately 1.000 to 1.013; this is a small cached CPU-dominated Python experiment on macOS 15.3.1 arm64, Python 3.12.14.

The candidate's displayed absolute median is 49.84% below the binary64 control's median. That is a descriptive cross-mode comparison, **not a directly bracketed candidate-versus-binary64 paired estimate**: both modes were bracketed by the frozen direct reference. A direct pair against the new strongest control, at larger scale and tighter tolerances, remains stronger evidence to collect. There is no extrapolation to Rust/native speed ratios or Neo4j.

The binary64 control also does fused input hashing during both residual passes, an exit runtime guard and a code-hash read. Those protocol costs are explicitly included in its timing but are not inherent requirements of residual arithmetic. They are disclosed rather than silently attributed to algorithmic cost. The private-driver wrappers and source/state validation policy are shared across the experiment.

### Work, State And Bytes

Both scalar schedules read a modeled 2,571,824 publisher bytes over three queries, versus 5,191,592 for either direct schedule: 50.46% less. All methods additionally hash/load 45,264 factor-state bytes and write the same 40,368 score bytes. The modeled publisher counters exclude small repeated headers, preparation JSON, the binary64 code-hash read, post-timing audit and OS/cache traffic. They are not physical disk-read counters.

The new row schedule does 200,000 binary64 row additions per query and 1,682 high-precision row enclosures; the former is not a count of all floating operations. Its full high-precision work is O(N+F), not O(N), because the final factor ledger remains high precision. Overall decoding/arithmetic remains O(Z+N+F). Formula-derived counters are labeled as such rather than presented as instrumentation.

The selected factor payload is 15,088 bytes for both the scalar candidate and the phased binary64 direct control. Their worker RSS ranges overlap each other and the original control. The public source has only F=943; interpreter/decoder effects dominate these small vectors. This experiment cannot substantiate a gigabyte-scale RAM reduction or any hard RAM ceiling.

### Accuracy Cost And Admission

Within each method/query, hashes and bounds repeat exactly across workers. Frozen direct and binary64 direct emit identical bytes. Precision placement changes the bytes compared with both direct and frozen scalar, as expected from the inherited lower-endpoint representative.

| Query | Precision-placed bound | Binary64 direct bound | Frozen scalar/Rump bound | Original Decimal audit bound on precision-placed bytes |
| --- | ---: | ---: | ---: | ---: |
| Uniform | 3.306e-13 | 2.065e-13 | 1.406e-13 | 3.705e-14 |
| Source 0 | 3.253e-13 | 2.262e-13 | 1.311e-13 | 3.814e-14 |
| Source 1681 | 3.289e-13 | 1.869e-13 | 1.479e-13 | 3.778e-14 |

All meet the requested 1e-10 bound. The audit bounds on ordinary direct bytes are approximately 9.805e-16 to 5.545e-15. These are conservative numerical upper bounds, not exact public-source errors. Raising precision cannot remove error introduced by the binary64 row-bound model or choosing a lower-endpoint score.

For the same saved state and current implementations, comparing these deterministic bounds with epsilon=1e-13 would refuse all three non-Decimal candidate modes while the original direct certifier's bounds pass. This is a derived admission comparison, not another timed tolerance experiment. A fresh solve could change residuals, but cannot be assumed to eliminate the certificate floors demonstrated by the exact-answer fixture.

### Preparation And The Product-Level Reading

This run reuses the exact earlier preparation: 632.319 ms for manifest construction, three upstream solves and score-to-factor mapping, with maximum preparer RSS 20.496 MB. The manifest alone previously cost 13.645 ms and occupies 9,298 bytes. Those measurements are historical preparation fields retained in the new receipt; they were not rerun or represented as free new work. Source import is earlier still and is not included in that number.

An illustrative composition makes the scope clear: adding that fixed historical 632.319 ms to the new publication medians gives approximately 800.178 ms for frozen direct, 760.195 ms for binary64 direct and 696.460 ms for the candidate. The apparent reduction then becomes 12.96% versus frozen direct or 8.38% versus binary64 direct, not 62%. **Those sums are arithmetic illustrations, not measured end-to-end sessions**; actual integrated solving may avoid the paid score-to-h mapping and change all phase costs. They explain why a stage result cannot be sold as a complete-workflow speedup.

## 9. Reproduction, Integrity And Next Research Action

```sh
/Users/amuldotexe/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -B \
  research_algorithms_20260920/experiments/benchmark_precision_publication_comparison.py \
  --store /tmp/knight-bus-ml100k-rows.bin \
  --folder /tmp/knight-bus-publication-study-20260921 \
  --receipt /tmp/knight-bus-precision-reproduction.json \
  --reuse-preparation --repeats 3
```

Omit `--reuse-preparation` and use a fresh external folder to pay and record preparation again. Preserve the separately licensed raw and prepared MovieLens source provenance from the native study; no raw dataset or individual answers were added to Git.

| Artifact | SHA256 |
| --- | --- |
| Precision publisher, unchanged after review | `8cde5a642b189e74c5a32320b5e1aceafd6fd2ae6afe518249485a00ac90cd82` |
| Precision tests after review additions | `5e28f730e5de0201eb5dee7b8c09c22a87948252f25a80b464db4c36aaa39abe` |
| Binary64 direct certifier | `ce919520838587b3fe966df5ff51f3692d02d954a9ee50605fcb795f65976e82` |
| Binary64 tests | `35a132f47700b285fe70570408b9c8d3c8e5fd3961678c5455cc6df268861973` |
| New comparison driver | `1d66838dff6a844db98def7ab3f74f89f9b74fdb3ce11f998166f8a905895f71` |
| New comparison tests | `1b8d397445577ab36c158c05598c3a42b792b7ac88eae5fd9fe88d7e190c1338` |
| New public receipt | `4c19d1c274563e47b0cec86ae451ed72719fd109d516ef35f56816812b9d16cb` |
| Reused preparation JSON | `4c4a550a8b6064d277a2832853d3f87ec49a7c907bef56dff3ce2c425898be71` |

All six code hashes in the receipt were checked against the current files, including the unchanged original probe, frozen publisher and frozen comparison driver. Both original receipt files remain untouched. The independent review snapshot predates only the new test additions and reports their earlier hash; it is intentionally preserved.

The next decisive work is an **accuracy/work/memory frontier**, not another unconditional speed claim:

1. Test a central row representative with the full interval-distance charge. The reviewed lower-endpoint bias is avoidable, but improving it must not drop row uncertainty or silently turn a negative interval into a certified positive scalar.
2. Compare compensated or exact row accumulation and a tighter ordinary direct control, with explicit reservation and no hidden fallback. The current adverse duplicate-factor fixture makes increased Decimal precision alone a non-solution.
3. Integrate a factor solver that actually exports its final h, eliminate or charge score-to-state mapping, and include full output, source validation and any extra solve work demanded by publication tolerance.
4. Add a lawful larger native source with more factors and a bounded importer/decoder. Measure phase peaks and complete-job physical memory. The current 16F equality makes the source-pass/accuracy frontier more plausible than a universal retained-state advantage.
5. Establish closest-art positioning for fused validated publication on graph factorizations and continue the other six missing scientific deltas. The inherited numerical theorem, ordinary strong comparator and successful engineering experiment do not by themselves meet the seven-family arXiv-worthiness requirement.

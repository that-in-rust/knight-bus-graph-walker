# PageRank: Integrated Solve, Accuracy And Publication Frontier

Date: 2026-09-21. Status: implemented; 40 focused tests, 108 accepted public comparison outputs, and a separate 15-query strict-admission experiment completed. This is A01 research within the seven-family objective. The [previous experiment](PageRank-Precision-Placement-Experiment.md) established a publication-stage speedup. This iteration measures actual CG solve through certified output for a prepared native source; it still does not establish a physical-RAM guarantee or new generic solver.

## 1. What This Iteration Must Resolve

Two earlier shortcuts could overstate product value. First, the three supplied factor states were produced by solving for scores, certifying those scores, and then rereading them to compute h. That paid mapping changed the candidate state and was not a free solver export. Second, the fastest row enclosure used its lower endpoint as the emitted representative, introducing avoidable bias and sometimes refusing an exact answer.

The new experiment must use the actual final conjugate-gradient h, release other Krylov arrays before publication, and count the solve together with complete output and certification. Separately, it must distinguish the choice of a representative inside an interval from the precision used to obtain that interval. A better representative does not make the uncertainty disappear.

The product question is consequently precise: for an already prepared native incidence source, which admitted numerical plan gives the best solve-to-complete-answer time under the requested error tolerance and declared factor-state reservation? Raw import, shared manifest construction, physical RAM and arbitrary Neo4j query compatibility remain separately scoped.

## 2. Three Accuracy Choices, Not Three Claimed Inventions

| Choice | Row-sum enclosure | Emitted representative | Expected advantage to test | Cost or risk |
| --- | --- | --- | --- | --- |
| Rump/lower | Published same-order signed/absolute-sum bound | Lower endpoint, as previously measured | Frozen behavior reference | Avoidable output bias plus uncertainty charge |
| Rump/midpoint | Same rigorous interval | Rounded interval midpoint | Remove endpoint bias without extra membership passes | Interval width still creates a certificate floor |
| Exact/midpoint | Exact dyadic sum followed by outward Decimal conversion | Rounded interval midpoint | Eliminate row-summation uncertainty | Integer work per membership; scatter error remains |

The [implementation](experiments/accuracy_frontier_rank_certificate.py) also exposes Decimal/lower and Decimal/midpoint controls. Matching lower-endpoint modes must reproduce the frozen publisher's bytes and every numerical bound component on finite fixtures. The two direct actual-output controls remain available: outward binary64 endpoints and the original Decimal endpoints. Neither is weakened to make the candidate look good.

Exact superaccumulation is established numerical analysis. [Neal's 2015 paper](https://www.cs.toronto.edu/~radford/ftp/xsum.pdf), especially the introduction and the discussion preceding the small accumulator, describes the fixed-point idea and much faster specialized implementations. Our Python big-integer row control is not Neal's implementation, and his measured speed ratios do not transfer here. Midpoint interval representations are likewise not new. The research opportunity must concern a useful graph-source-specific resource/accuracy schedule and complete workflow, not priority for these primitives.

## 3. Exact Row Sum With A Bounded Numeric Range

Every finite binary64 h_j is an integer multiple of eta=2^-1074. Obtain its exact integer ratio n_j/d_j, where d_j is a power of two, and accumulate

```text
I = 0
for each factor j in this row:
    n, d = exact_integer_ratio(h[j])
    I += n << (1074 - log2(d))

exact row sum = I / 2^1074
```

The logarithm above is implemented as `d.bit_length()-1`, not a floating logarithm. This handles signs, signed zero, subnormals and catastrophic cancellation. Integer arithmetic can represent a finite-input sum outside the binary64 range; later publication may still refuse a nonfinite chosen float. The exact accumulation has no membership-wise Decimal rounding. Outward Decimal division is applied only after obtaining the exact integer.

For q>=1 summands, each magnitude is below 2^1024. Therefore the magnitude of the accumulator is strictly below q*2^2098 and needs at most `2098+ceil(log2(q))` bits. This is a bound on the numeric accumulator, **not a whole-process byte allocation bound**. Python integers, the shifted term, exact-ratio numerator/denominator, old/new accumulator objects, Decimal conversions and interpreter overhead coexist transiently. The original row count is a uint32 field, so valid stored rows have a fixed finite count range; the full decoded row remains O(q) state.

This changes row arithmetic, not the PageRank target. The operator remains A=BB^T-Q with binary native factors, exact damping 17/20, retained vertex universe and declared dangling policy. The [original scalar-ledger proof](PageRank-Streaming-Publication-Certificate.md) supplies the residual identity and error propagation. It does not cover arbitrary directed weighted Neo4j graphs without a faithful factor/defect extension.

## 4. Why Midpoint Selection Is Safe But Not Magic

Given an enclosure [lo,hi] for the exact row value y, choose any finite admitted yhat. The maximum of the two outward differences bounds its error:

```text
delta >= max(yhat-lo, hi-yhat, 0)
```

The midpoint minimizes that worst-case distance over real representatives; rounding and the finite-precision midpoint computation are still charged by evaluating the distance of the actual chosen float. No interval endpoint is replaced by the midpoint in the proof. No uncertainty is discarded. The scatter input retains the nonnegative normal-or-zero admission policy; the implementation does not silently clip a negative center to zero.

The frozen ledger then charges `sum_i (d_i+q_i)*delta_i`, the weighted scatter roundoff, the observed factor residual and the difference between actual serialized scores and ideal reconstruction. The final bound remains

```text
Efinal = (17/3)*(Rhat + Escatter + Erow) + Epub.
```

Two-vertex duplicate-factor fixtures distinguish the mechanisms. With q a power of two and exact h_j=1/q, the stationary answer is exactly (0.5,0.5). Rump/lower perturbs those bytes. Rump/midpoint restores them but still pays a conservative row-width charge. Exact/midpoint restores the bytes and removes that row-sum charge; it can meet 2e-15 in the retained fixtures. None of these statements removes the scatter certificate's own floor.

## 5. Actual Solver State, Not A Second Projection

The separate [state exporter](experiments/factor_rank_state_solver.py) implements the existing factor-CG initialization, operator order and recursive residual screen. It is ordinary CG, not a new solver. A fresh helper frame owns h, counts, RHS, residual, direction and operator product. Only the final h and scalar metrics escape; the other arrays are released before the caller publishes. Final acceptance still comes from the publisher, because the recursive floating residual screen is not a rigorous certificate.

```text
validated prepared source
          |
          v
    ordinary factor CG         selected phase payload: up to 48F
          |
          | return actual h; release other arrays
          v
    publication choice         scalar or binary-direct: up to 16F
          |
          v
    complete score file + certificate
```

These are logical selected-array payloads, not measured physical peaks. The pipeline must not retain copies of h or solver tracebacks containing all arrays. For the direct binary64 control, the exclusively owned h is consumed before the two endpoint arrays are allocated. The Decimal direct path still has a different endpoint representation. The full process also retains interpreter, row-decoder, file-buffer and scalar state.

The [integrated driver](experiments/benchmark_integrated_accuracy_rank.py) executes three personalizations per fresh worker. It validates source identity, solves directly into h, hashes h incrementally without joining an F-sized collection of byte objects, publishes, and releases h. It never calls the old score-producing solver and never writes intermediate factor-state or upstream-score files. Independent actual-output audits run after session timing and pre-audit RSS measurement, with post-audit costs reported separately.

## 6. Predeclared Experiments

Use the existing licensed MovieLens-derived native source with N=1,682, F=943 and Z=100,000. Build and record a fresh trusted manifest separately. Keep raw source and all individual scores outside Git.

For the main epsilon=1e-10 comparison, the strongest newly implemented direct control, `binary_direct`, brackets every candidate. Candidates are `rump_lower`, `rump_midpoint`, `exact_midpoint` and `decimal_direct`, with three rotated repetitions each. Thus the planned run has 12 triplets, 36 fresh workers and 108 complete answers. All outcomes remain. The existing 15% endpoint-consistency rule excludes unstable ratios without deleting the observations. No retries or adjustment based on favorable timing.

Then run one explicitly non-ranked admission session per method at epsilon=1e-13. Every method uses the same solver epsilon within its run. A refused query's elapsed work and partial metrics remain visible but must never be ranked as a fast successful answer. No automatic fallback or extra memory reservation is hidden. This strict-tolerance run is a separate experiment, not additional favorable repetitions of the timing study.

Measure solve and publication phase time, complete-session wall/CPU, exact final-state hashes, solver iterations, actual-output hashes, numerical bound components, accepted/refused outcomes, modeled source traffic, logical selected arrays and worker RSS before/after independent audit. State hashes and iteration counts should match across publication choices for the same tolerance and query. Raw import and the shared manifest build are not inside per-session timing; keep that boundary explicit.

## 7. Initial Verification And Scope

Fifteen accuracy-frontier tests failed before the module existed, then passed in 0.709 seconds. They include 960 inherited exact full-output cases and 288 additional complete outputs across all six row-sum/representative combinations. Exact-sum tests cover 106 deterministic cases, mixed exponents through maximum finite values, subnormals, reversal invariance, outward enclosure at precisions 20/50/100 and nonfinite refusal. The duplicate-factor and signed-cancellation fixtures verify the numerical purpose of the new variants rather than merely requiring a Boolean success.

Five integration tests were written and failed on the absent driver. Their contract includes identical actual solver states across methods, complete outputs/audits, no old score-solver invocation or intermediate files, preserved old answers on refusal, source-identity rejection and retention of completed triplets after worker failure. Exact current executed statuses, reviewer findings and public results belong in the completed-results section, not inferred from these planned assertions.

The prior seven-family completion gates remain open. This iteration can produce useful implementation and empirical evidence, but cannot establish publication-worthiness by calling an exact accumulator novel or by comparing only with a high-overhead numerical reference. The separate [A05 closest-art assessment](Similarity-Orientation-Prior-Art-Assessment.md) supplies a proposed additive-region contraction, not a completed seven-family result.

## 8. Completed Review And Regression Integration

The [independent accuracy review](PageRank-Accuracy-Frontier-Review.md) found no soundness defect under the stated immutable validated input, trusted manifest, nearest/even and gradual-underflow conditions. It is a bounded review, not formal verification. Two P3 reporting findings were reproduced before repair: missing identity-read keys and ambiguity about arbitrary-precision integer work. The receipt now preserves both frozen identity counters and separately reports Decimal membership additions and exact-integer membership accumulations. The legacy high-precision counter explicitly means Decimal-only work.

The [exporter note](PageRank-Factor-State-Integration.md) and its test were relocated into this research folder; their temporary root locations were accidental. No frozen solver or publisher was changed. The combined lead command passed **40 tests in 1.230 seconds**: 17 accuracy-frontier, 18 actual-state export/ownership, and five integration tests. The tests include 512 tiny actual-CG-state comparisons with the frozen solver; the accuracy tests retain 960 inherited and 288 six-variant complete outputs. These counts overlap by purpose and are not independent datasets. The review's additional 30 variant safety checks are separate reviewer evidence. The integration driver's synthetic 1-ms worker fixtures test receipt persistence, not performance.

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -B -m unittest \
  test_accuracy_frontier_rank_certificate.AccuracyFrontierCertificateTests \
  test_factor_rank_state_solver.FactorStateSolverTests \
  test_integrated_accuracy_rank_comparison.IntegratedAccuracyComparisonTests
```

## 9. Measured Solve-To-Result Comparison

Both predeclared runs completed serially, with the earlier agents closed and no concurrent local test suite or research benchmark. This does not prove absence of unrelated machine activity. The main run retained all **12 triplets / 36 workers / 108 complete answers**; every answer passed its publication gate and post-timing original-operator audit. All twelve endpoint pairs passed the predeclared 15% screen. Three candidate observations per method are descriptive evidence, not a population confidence interval or a tail-latency estimate.

For the fixed prepared source N=1,682, F=943, Z=100,000 and epsilon=1e-10:

| Plan | Median locally paired time / binary-direct control | Median three-query session ms | Median solve sum ms | Median publication sum ms | Pre-audit peak RSS range, decimal MB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Binary64 direct | 1.000 reference, 24 workers | 533.744 | 400.401 | 128.894 | 21.250-23.331 |
| Rump/lower | 0.8731, three workers | 466.428 | 400.796 | 65.159 | 21.479-22.331 |
| Rump/midpoint | 0.8321, three workers | 462.755 | 396.256 | 66.038 | 21.053-22.512 |
| Exact/midpoint | 0.9739, three workers | 508.110 | 392.427 | 115.202 | 21.692-22.053 |
| Decimal direct | 1.0992, three workers | 574.573 | 402.766 | 171.318 | 22.217-22.381 |

The paired ratios use each candidate's bracketing controls, not a ratio of the medians in the next column. Phase medians need not add to session medians. Session identity validation, bookkeeping and incremental state hashing are paid in session time. The row variants' median paired reductions are **12.69%, 16.79%, and 2.61%**, respectively; Decimal direct is **9.92% higher**. This does not establish a significant speed ranking between lower and midpoint: only three observations, shared solve cost and machine noise remain. The strong supported distinction is that approximately halving publication work produces a much smaller complete-solve improvement.

Every query uses 11 CG updates in this run. For each source, the final h hash is identical across all five plans and all repetitions. The two direct controls emit identical bytes. The three scalar variants emit different bytes, each checked against the same stationary PageRank target and requested L1 error tolerance. No plan silently performs more solver iterations to buy admission.

| Personalization | Actual final h SHA-256 |
| --- | --- |
| Uniform | `6977a5d1c47f2203bbc5c65a7827eb45149b06002310f677abcb6182d37cb00a` |
| Vertex 0 | `4066a2b3fddc06aa38f4ae59196b45f02dcb12453cac87fb6febb0c8d8d921ac` |
| Vertex 1681 | `d9a635eb9b1dee6f355ec261242a0c5bf60d191db864c79df223d9892c77e028` |

All methods reserve 48F=45,264 selected solver-array bytes. Scalar and phased binary-direct publication use 16F=15,088 selected bytes; the full workflow does not add sequential phase reservations together. The RSS ranges overlap: **no measured whole-process RAM reduction is established**. Row materialization, interpreter state, allocator effects and unbounded raw-source preparation remain outside a physical-budget proof.

Modeled three-query reads, including the one session identity check, fall from **22,009,626** direct bytes to **19,389,858** scalar bytes, approximately **11.90%**. Solver reads alone are 16,388,520 bytes in either plan. Publication reads fall from 5,191,592 to 2,571,824, or 50.46%, but that stage percentage must not be relabeled as the whole-workflow percentage. These are logical payload counters, not physical disk bytes; output writes are 40,368 bytes. Shared manifest setup was separately measured at 15.316 ms, with 1,269,946 modeled read bytes. Raw dataset download/import and storage construction are still excluded from these numbers.

## 10. Strict Accuracy Changes The Admitted Plan

The separately declared epsilon=1e-13 study performs one fresh worker per plan. Its 15 attempts are **not** latency-ranked because twelve refuse. The solver uses 12/13/13 updates for uniform/first/last source, with identical h per source across plans.

| Plan | Certified answers / attempts | Returned L1 certificate range | Meaning |
| --- | ---: | ---: | --- |
| Binary64 direct | 0/3 | 1.898e-13 to 2.261e-13 | Its outward arithmetic is too conservative here |
| Rump/lower | 0/3 | 3.248e-13 to 3.406e-13 | Endpoint bias and row/scatter uncertainty remain |
| Rump/midpoint | 0/3 | 2.018e-13 to 2.215e-13 | Better representatives do not erase uncertainty |
| Exact/midpoint | 0/3 | 1.328e-13 to 1.542e-13 | Exact row accumulation does not remove scatter-ledger error |
| Decimal direct | 3/3 | 1.100e-15 to 7.557e-15 | All three also pass the independent audit |

**A refusal is not proof that the underlying candidate answer is inaccurate.** It means this publisher could not establish the requested bound. Candidate hashes and bounds remain in the receipt, but refused score files are not published and receive no independent output audit. Only the three Decimal accepted outputs are independently audited. There is no hidden rescue run or favorable deletion of refusals.

The current product implication is a certified numerical-plan portfolio, not a claim that the fastest option always works. A paid fallback could reconstruct and directly certify from retained h without solving again, but that fallback is **not implemented or measured here**. It must account for retained ownership, different endpoint memory, temporary output, extra scans, and rejected work. The existing exact/midpoint prototype is not a successful general rescue for the strict public workload.

## 11. Reproduction And Frozen Receipts

Source file: `/tmp/knight-bus-ml100k-rows.bin`, SHA-256 `9e4acc25fc0b988f2a06d4f0e793221e7133c1ed8fb02d71096c78a73e45bdec`. Follow the existing MovieLens source recipe and licensing limits; raw data and individual outputs stay outside Git. This is weighted clique-projection PageRank from native memberships, not arbitrary GDS property-graph semantics.

Run from repository root with the bundled Python 3.12.14 runtime:

```sh
python3 -B research_algorithms_20260920/experiments/benchmark_integrated_accuracy_rank.py \
  --store /tmp/knight-bus-ml100k-rows.bin --folder /tmp/knight-bus-integrated-study-20260921 \
  --receipt research_algorithms_20260920/PageRank-Integrated-Accuracy-Results.json \
  --repeats 3 --epsilon 1e-10
python3 -B research_algorithms_20260920/experiments/benchmark_integrated_accuracy_rank.py \
  --store /tmp/knight-bus-ml100k-rows.bin --folder /tmp/knight-bus-integrated-strict-20260921 \
  --receipt research_algorithms_20260920/PageRank-Integrated-Admission-Results.json \
  --epsilon 1e-13 --admission-only
```

Use a new receipt path for a new execution; preserve the measured receipts. The literal `python3` above denotes the bundled interpreter, not the older macOS system interpreter. Module hashes for all nine implementation dependencies are embedded in each receipt.

| Artifact | SHA-256 |
| --- | --- |
| [Main receipt](PageRank-Integrated-Accuracy-Results.json) | `2d70b3a7ad3937031eafcaa9eea46811019c366e03cf8c83a6d39141691900e0` |
| [Strict receipt](PageRank-Integrated-Admission-Results.json) | `48807ad2c9034f7508ac4e21a2a3fecb5978af20de4890d509def8ec3b2caf09` |
| Integrated driver | `1a2c8f02afa859fe9efe0d90c8ee03145b68a83581b5791e60a6a4b3f3fd96cb` |
| Accuracy publisher after reporting fixes | `04d8737ed97ee5f20436b2e7f42187df346b06b7e04c0c892fe91bae33e1d464` |
| Actual-state exporter | `52077fc70af663551195594971a8826115b1c3eb92f96609093189e177006920` |

## 12. What Changed After Challenging The Claim

1. Earlier publication-only ratios overstated what they could say about the full solve. Actual integration now measures a roughly 17% paired reduction for midpoint, not a 62% whole-PageRank reduction.
2. A lower endpoint was a numerical choice, not part of the graph proof. Midpoint fixes avoidable output bias but leaves a measurable admission floor.
3. Exact accumulation sounded like a complete accuracy fix. The strict public experiment falsifies that: the scalar scatter ledger can still refuse while direct Decimal certification succeeds.
4. Eliminating Decimal vectors sounded like a memory win. The strongest phased binary64 control has the same selected payload, and RSS overlaps.
5. None of these engineering improvements demonstrates a new PageRank algorithm, broad source compression or seven publishable contributions. The next scientific step remains a genuinely differentiating bound/workload, while the next A01 product step is honest paid plan selection across tolerance and memory constraints.

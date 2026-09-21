# Accuracy-Frontier Independent Review

Date: 2026-09-21. Reviewed working-tree content at HEAD `5de087e37beb55b0db27ab17d77c3c59cd8749d8`; the research files are untracked, so the SHA-256 identities below, not HEAD alone, identify this review. Only this document was authored. No implementation/test edits, public-data runs, performance measurements, or long tests were performed. Synthetic files were confined to temporary directories.

## Verdict

**Qualified support: no certificate-soundness defect found under the binary64 nearest/even, gradual-underflow, trusted-manifest, immutable-source/state contract.** Exact dyadic accumulation, its magnitude-bit bound, outward Decimal conversion, and the full error charge for the selected midpoint are consistent. The copied publisher retains the material same-buffer and atomic-refusal checks.

The remaining actionable issues are two low-priority receipt/reporting discrepancies. The lead added a separate actual-new-entry live-mode regression during review; it was inspected and passed independently, closing that coverage gap. None of the findings demonstrates an understated numerical certificate. Ordinary exact accumulators/superaccumulators and midpoint representatives are established numerical techniques, not a new theorem or a new PageRank algorithm. This review provides code inspection and finite witnesses, not formal verification or a priority claim. The integrated driver is outside this review.

## Findings

### F1 [P3] Exact-mode work counter needs a Decimal-only scope

At [receipt line 180](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/accuracy_frontier_rank_certificate.py:180), `high_precision_membership_operations` is zero for exact mode. Yet [lines 28-35](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/accuracy_frontier_rank_certificate.py:28) perform an arbitrary-precision integer shift and accumulation per membership. The counter can validly mean zero **Decimal additions** per membership; it cannot mean zero high-precision membership work. An ordinary value `1.0` already has a 1075-bit coefficient in these units.

Action: label this counter explicitly as Decimal work and report exact-integer membership work separately, or document that narrower interpretation in the receipt. The magnitude-bit upper bound is useful but does not repair the ambiguous operation count. Do not infer whole-publisher O(N) high-precision work, a speedup, or physical-memory savings from zero in this field.

### F2 [P3] Copied receipt drops two frozen identity-read fields

The [new receipt](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/accuracy_frontier_rank_certificate.py:164) omits `source_identity_read_bytes` and `metadata_identity_read_bytes`, present in the [frozen receipt](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/streaming_rank_publication_certificate.py:250). A one-factor default decimal/lower call reproduced exactly those two missing keys. Identity hashing still executes and the aggregate `logical_read_bytes` formula is retained: this is receipt-schema drift, not skipped validation.

The module exposes the old entry-point name as an alias at line 194, while [the parity test](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_accuracy_frontier_rank_certificate.py:115) compares only five numerical/hash fields. Restore the counters if that alias is intended to preserve the frozen receipt contract; otherwise explicitly declare the schema difference. Add stable-key parity coverage excluding intentional new fields and timings. No failing downstream consumer was demonstrated in this bounded review.

### Live-mode gap addressed during review

The inherited child explicitly [imports the frozen publisher](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_streaming_rank_certificate.py:206). It remains valid historical **frozen coverage only**. The lead's separate [actual-new-entry regression](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_accuracy_frontier_rank_certificate.py:131) now imports accuracy-frontier and calls `publish_accuracy_frontier_certificate` for all three row modes with midpoint, under upward, downward, toward-zero, and Darwin ARM64 FZ. It checks actual FZ bits, preserves prior output, restores the environment, and has a child timeout. The updated 16-test accuracy class passed independently. No publisher code changed.

Additional manual review evidence covered **both** representatives: all six combinations admitted nearest/even and refused all four bad modes, giving **6 admissions and 24 atomic refusals passed**. Every refusal preserved the prior file and left no temporary file. That broader manual matrix is not counted as a persisted regression. Separate x86 FTZ-only/DAZ-only coverage remains open; it does not invalidate the new Darwin regression.

## Numerical Checks

### Exact coefficient and bit bound

For a finite binary64 value, `as_integer_ratio()` returns `a / 2^k`, with `0 <= k <= 1074`. Thus `a << (1074-k)` is an exact signed coefficient in units of `2^-1074`. The code uses `denominator.bit_length()-1` for k, including denominator 1. Signed zero becomes integer zero; losing its sign changes no real-valued error assertion. Nonfinite states are refused before accumulation. The exact-ratio API behavior is documented in the [Python 3.12 reference](https://docs.python.org/3.12/library/stdtypes.html#float.as_integer_ratio).

The largest finite magnitude has coefficient `(2^53-1) * 2^2045 < 2^2098`. For q >= 1 signed summands, the triangle inequality gives `abs(total) <= q * ((2^53-1) * 2^2045) < q * 2^2098`. Therefore magnitude bit length is at most `2098 + ceil(log2(q))`. For positive integer q, `(q-1).bit_length()` is exactly that ceiling. The receipt takes maximum decoded row q and explicitly returns zero when no row has memberships. The bound excludes a sign representation, Python object overhead, shifted operands, and simultaneous temporaries; it is not an allocation measurement.

Independent checks covered 4,202 single-value identities, including both signs of every power of two from exponent -1074 through 1023, signed zeros, the least subnormal, and maximum finite values. The committed mixed-mantissa/exponent test adds 106 vectors, including overflowing real sums and cancellation, at precision 20/50/100.

### Outward conversion and reconstruction

`Decimal(integer)` and `Decimal(1 << 1074)` preserve their integer values; context precision applies to the subsequent explicit floor/ceiling divisions, not these constructor inputs. This distinction is supported by the [Decimal constructor/context documentation](https://docs.python.org/3.12/library/decimal.html#decimal.Decimal). No intermediate binary64 sum or float conversion is used in the exact row sum.

For all three modes the row target remains `(20*b_i + 17*sum(h_j)) / (20*d_i + 17*q_i)`. Positive coefficients and the exact positive integer denominator preserve endpoint order, including negative sums. Decimal mode delegates to the frozen helper. Rump mode delegates to the unchanged same-order signed/absolute helper and retains its nearest/gradual-underflow/no-overflow requirements. Exact mode removes row-accumulator rounding, not subsequent Decimal, binary64, scatter, or publication errors.

The explicit contexts retain the ordinary default Decimal exponent range, which easily contains these binary64-derived values and integer row metadata at tested precision. This review does not extend the contract to hostile mutation of `decimal.DefaultContext`, dependencies, or process arithmetic during a call.

Independent exact-mode checks used seven cancellation/overflow/subnormal vectors, precision 20/50/100, and degrees `q`, `2^53+3`, and `2^64-1`: **63 reconstructed Fraction enclosures and 126 representative-distance checks passed**. Large degrees here are helper-level algebra tests, not claims of constructing a huge valid graph.

### Midpoint retains the full error charge

[Lines 55-57](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/accuracy_frontier_rank_certificate.py:55) compute a rounded Decimal midpoint and then convert it to binary64. It need not be the exact midpoint or always lie inside a very narrow interval. Soundness does not require that: [lines 124-125](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/accuracy_frontier_rank_certificate.py:124) measure the **actual selected binary64 value** against both original endpoints, rounding upward. The charge remains `(d_i+q_i)*delta_i`; it is not replaced by half-width or rounded away.

Scatter uses that same `yhat`. The published `d_i*yhat` is packed, unpacked, and charged against outward `d_i*[lo,hi]`. Isolate representatives receive the analogous emitted-value charge. The final formula remains upward `17/3 * (factor_residual + scatter_error + row_error) + publication_error`. Neither midpoint selection nor exact row accumulation discards a ledger term. No shared frozen/placed bindings were replaced by importing or calling the candidate in the independent witness.

## Accuracy And Overflow Witnesses

All duplicate-factor comparisons fixed h, row order, tolerance, and **Rump scatter**. For two vertices and q copies of `[0,1]`, `h_j=1/q`, the exact answer is `(0.5,0.5)`. Precision was 50 and epsilon was `2e-15`:

| q | Row/representative | Final upper bound | Admission |
| --- | --- | ---: | --- |
| 2 | Decimal/lower or midpoint | 1.258252761242e-15 | accept |
| 2 | Rump/lower | 4.514906966809e-15 | refuse |
| 2 | Rump/midpoint | 2.516505522484e-15 | refuse |
| 2 | Exact/lower or midpoint | 1.258252761242e-15 | accept |
| 64 | Decimal/lower or midpoint | 1.258252761242e-15 | accept |
| 64 | Rump/lower | 2.322956641857e-13 | refuse |
| 64 | Rump/midpoint | 8.052817671948e-14 | refuse |
| 64 | Exact/lower or midpoint | 1.258252761242e-15 | accept |

The same six-way admission sweep also passed for q=4 and 16. At loose tolerance the committed midpoint test verifies exact `(0.5,0.5)` bytes for Rump/midpoint; nevertheless its retained row/scatter bound still misses `2e-15`. Better emitted accuracy is not automatically better enough for admission. Exact mode removes this row-bound floor but not the conservative scatter floor. Every independent refusal preserved the existing output.

The previous zero-row witness is now directly covered: three vertices, two `[0,1]` factors, personalization at isolate 2, `h=[2^-100,-2^-100]`, precision 100, epsilon `1e-20`. Both Rump/midpoint and exact/midpoint accept and emit `(0,0,1)`. That addresses this witness, not a general guarantee that midpoint can admit every signed state or that its score is always more accurate.

Exact-mode overflow distinctions were checked independently, not inferred from the accumulator unit test:

- Two duplicate factors with `h=[DBL_MAX,DBL_MAX]` have a real row sum beyond binary64 range. Both exact representatives nevertheless produce finite scores `0x1.d67c8a60dd67cp+1023`, return a 2099-bit accumulator bound, and enclose actual Fraction L1 error when explicitly allowed the very loose `Decimal('1e310')` tolerance. Such huge scores are an arithmetic stress case, not useful normalized ranking output.
- Three duplicate factors with `h=[DBL_MAX]*3` keep the selected y finite but overflow `degree*yhat`. Both representatives raise `ValueError('output overflow')`, preserve the prior output, and remove the temporary file. Exact summation is not a promise of finite publication.
- Four duplicate factors, isolate personalization, and `h=[DBL_MAX,DBL_MAX,-DBL_MAX,-DBL_MAX]`: Rump refuses row-summation overflow; exact/midpoint accepts finite `(0,0,1)` at deliberately loose `Decimal('1e311')`. Its large residual charge is retained despite zero emitted oracle error.

These loose Decimal tolerances also expose an intentional-looking API difference: new validation accepts finite positive Decimal values above the binary64 range, whereas frozen validation first calls `math.isfinite(epsilon)`. The new strict type gate rejects bool, Fraction, strings, and numeric subclasses. Reproduction of decimal/lower output on ordinary inputs does **not** establish identical parameter-admission semantics. The additional gradual-underflow guard also deliberately makes decimal/lower mode stricter than the frozen guard alone.

## Publication And Trust Boundary

The copied loop retains source/metadata path exclusion, identity checks before publication, finite state validation, the 16F payload gate before scatter allocation, finite normal-or-zero nonnegative y admission, scatter/output overflow checks, same encoded-buffer hashing, short-write detection, final `8*N` length check, and same-directory `os.replace` only after acceptance. Receipt source stats are captured before replacement. A `finally` removes uncommitted temporary output. Refusal leaves an existing output intact; its old bytes are not being certified, and `output_sha256` is null while the candidate digest can still be reported.

The candidate hashes the bytes it passes to the file writer and charges the unpacked buffer. It does not reread storage, fsync, guarantee durability, or atomically commit an external receipt alongside the output. Those are inherited limits, not newly supplied protections.

Crucially, a matching hash is identity, not proof that a caller-supplied manifest is honest. The trusted builder establishes cardinalities, isolate flags, and `d_i = sum_j(s_j-1)`; publication checks identities/header consistency but does not rederive these relations. The row-ledger exchange depends on exactly those relations. Source, metadata, manifest, h, code bindings, and arithmetic mode must remain unchanged throughout publication. Concurrent mutation, forged-and-rehashed metadata, hostile file replacement, and nonconforming encoders/storage are outside this certificate. No new defense against them should be claimed.

Exact mode still retains h/scatter payload 16F, a full decoded row, and additional arbitrary-precision scalar/temporary storage. The complete publisher has per-factor and per-vertex Decimal ledger work as well as exact-integer work per membership. No physical RAM, lifecycle memory, 4-GB feasibility, throughput, or energy conclusion follows from these receipts.

## Test Commands And Results

Working directory for Python commands:
`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments`.
Runtime: Python 3.12.12, Darwin ARM64. `-B`/`PYTHONDONTWRITEBYTECODE=1` prevented bytecode writes.

```sh
env PYTHONDONTWRITEBYTECODE=1 /opt/homebrew/bin/python3.12 -B -m unittest -v \
  test_accuracy_frontier_rank_certificate.AccuracyFrontierCertificateTests \
  test_precision_placed_rank_certificate.PrecisionPlacedCertificateTests \
  test_streaming_rank_certificate.StreamingCertificateTests
```

Initial result: **42 passed, no skips**, exit 0: accuracy-frontier 15 (5 new, 10 inherited), current precision-placement 17, frozen 10. These are not 42 independent tests of the accuracy-frontier implementation. After the lead added its actual-new-entry regression, `env PYTHONDONTWRITEBYTECODE=1 /opt/homebrew/bin/python3.12 -B -m unittest -v test_accuracy_frontier_rank_certificate.AccuracyFrontierCertificateTests` independently passed **16 tests, no skips**, exit 0. The three-class command was then repeated with `-q` instead of `-v`: **43 passed, no skips**, exit 0 (16 accuracy-frontier, 17 precision-placement, 10 frozen).

The new all-variant test covers 16 graphs x 3 personalizations x 6 choices = 288 complete publications, with Fraction error and direct original-operator certification. Most committed end-to-end calls use precision 50 and Rump scatter for the new choices; inherited default calls exercise decimal/lower with both scatter bounds. The current precision-placement test file has additional regressions since the previous review's recorded hash.

This additional command was executed verbatim to target five safety/bound methods at **each** actual variant rather than only the default alias:

```sh
env PYTHONDONTWRITEBYTECODE=1 /opt/homebrew/bin/python3.12 -B - <<'PY'
import functools, itertools, types, unittest
import accuracy_frontier_rank_certificate as candidate
from test_streaming_rank_certificate import StreamingCertificateTests
suite = unittest.TestSuite()
methods = ['test_refusal_preserves_existing_output','test_manifest_validates_source_degrees',
           'test_receipt_io_precedes_commit','test_encoder_error_is_charged','test_arbitrary_states_preserve_bound']
for row_sum,representative in itertools.product(('decimal','rump','exact'),('lower','midpoint')):
    facade = types.SimpleNamespace(**vars(candidate))
    facade.publish_streaming_rank_certificate = functools.partial(candidate.publish_accuracy_frontier_certificate,
        row_sum=row_sum,representative=representative)
    def load_required_certificate_module(self, module=facade):
        return module
    variant = type(row_sum+'_'+representative,(StreamingCertificateTests,),
                   {'load_required_certificate_module':load_required_certificate_module})
    suite.addTests(variant(method) for method in methods)
result = unittest.TextTestRunner(verbosity=1).run(suite)
assert result.wasSuccessful()
PY
```

Result: **30 passed**, exit 0. No tests were added to source by this reviewer. Three additional stdin commands used the same `env ... python3.12 -B - <<'PY'` invocation; their executed fixtures/results are recorded in the numerical/admission sections above:

| Independent stdin command | Result |
| --- | --- |
| Binary64 grid, Fraction row/distance checks, q=2/4/16/64 six-way admission, q=2/3 extreme states, receipt key difference, invalid tolerances, import/call binding preservation | `INDEPENDENT ARITHMETIC / ADMISSION / RECEIPT WITNESSES PASS`, exit 0 |
| Actual accuracy-frontier live-mode calls for every row/representative choice | `ACTUAL NEW ENTRY: 6 nearest admissions, 24 directed/FZ atomic refusals PASS`, exit 0 |
| Four-term maximum-value signed cancellation | `max/max/-max/-max: Rump atomic refusal, exact finite certified output PASS`, exit 0 |

The live-mode command ran in its own interpreter and restored `fenv_t` in `finally`. It used Darwin ARM64's `(fpsr, fpcr)` two-uint64 layout, clearing mask `0x1c00000` before setting each of `0x400000`, `0x800000`, `0xc00000`, `0x1000000`. It checked `fegetround()` for the directed cases and raw `tiny+tiny` bits for FZ before calling **`candidate.publish_accuracy_frontier_certificate`**. No timing fields were used as performance evidence.

Other review commands: scoped graph discovery (new file not indexed, then direct named-file reads), `git status --short`, `git rev-parse HEAD`, `nl`/`sed`, `diff -u` (exit 1 means the expected source differences), and `env LC_ALL=C LANG=C shasum -a 256` on the files below. One initial `wc` used the wrong top-level experiments path and exited 1; actual files were then located under the research directory. No tests failed. External checking was limited to the official Python ratio/Decimal semantics cited above, not a literature survey.

## Remaining Gaps

- The actual-new-entry Darwin midpoint regression is now present and passing. Keep its coverage distinct from the inherited frozen child. Lower-representative live-mode coverage remains manual; separate x86 FTZ/DAZ modes were not executed here.
- Make exact-mode operation-counter scope and receipt compatibility explicit. Test stable receipt fields and the newly strict tolerance contract, not just five successful numerical fields.
- Preserve the independent extreme-state output-overflow and tight-tolerance same-h witnesses as focused regressions. Committed accumulator overflow coverage alone does not test the full publication path.
- This review exercised representative bit boundaries, not every mantissa/order or a general optimality/monotonic-admission claim. Midpoint and exact summation are controls, not guaranteed universally smallest-error outputs or tight certificates.
- No independent outward-binary64 direct-residual implementation, compensated-sum comparator, standalone scatter-overflow stress matrix, injected short-write/replace failure campaign, x86 mode coverage, or concurrent mutation defense was established. The existing Fraction/direct-certifier comparisons and scoped safety tests are not substitutes for those distinct checks.

## Reviewed Hashes

SHA-256 captured before tests and rechecked. The publisher and dependencies stayed unchanged; the lead updated only the accuracy test file within this review's source set. Its initial hash was `09e1da48105b43b5a52ca2fac48a57366251f4d1a095ccf10ee2eb74cbce3c87`; the final inspected and rerun test hash is below. All Python paths are within `research_algorithms_20260920/experiments/`; the previous review is in its parent.

| File | SHA-256 |
| --- | --- |
| accuracy_frontier_rank_certificate.py | `1a62428f601c899f201f76526442f0e8d1d10843586e0758341c1fadc6a03376` |
| test_accuracy_frontier_rank_certificate.py | `a2a6f29be1b1f672490233a7877552c1505b392078a4aeb735d34f239d34ccb2` |
| streaming_rank_publication_certificate.py | `022f3e8cea9ca870d7042bd2497818954705c85d71c93a27e5177e20d7de807a` |
| precision_placed_rank_certificate.py | `8cde5a642b189e74c5a32320b5e1aceafd6fd2ae6afe518249485a00ac90cd82` |
| test_streaming_rank_certificate.py | `d926d0aadd0227380d3cf67abde1b9eb548aa69b093c10172df5747c2d975dc8` |
| test_precision_placed_rank_certificate.py | `5e28f730e5de0201eb5dee7b8c09c22a87948252f25a80b464db4c36aaa39abe` |
| probe_native_incidence_pagerank.py | `d09558f63a906ccfdb859f7db6272caf1110474182b96fa7de088dfc3db64b65` |
| PageRank-Precision-Placement-Review.md | `b9260ba960e57b87ee654633da414898abe3a6bb021213c260ad0afa178d7be1` |

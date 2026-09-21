# Binary64 Direct Residual Certificate

## Status and Contribution

Finished ordinary reference control, not a new PageRank algorithm or novelty
claim. The implementation replaces the frozen direct certificate's two Decimal
factor vectors with two `array('d')` endpoint vectors. It still evaluates the
original operator on the **actual complete serialized binary64 output**, using
two source scans and two output scans. There is no solver, reconstruction,
renormalization, factor-state ledger, or publication in this API.

Owned artifacts:

- `experiments/binary64_rank_residual_certificate.py`
- `experiments/test_binary64_rank_residual_certificate.py`
- This document.

The frozen `probe_native_incidence_pagerank.py` direct certificate and
`streaming_rank_publication_certificate.py` runtime/manifest helpers were read
and left unchanged. Arithmetic does not call either certificate. The control
reuses only the frozen incidence header reader and binary format constants.
No commits, pushes, public-data runs, or long suites were performed.

## API and Preconditions

```python
certify_binary64_rank_output(path, output, source, epsilon, precision=50)
```

`path` is a `KBINC001` incidence store. `output` contains exactly N little-endian
binary64 values, with no header. `source=None` selects exact uniform
personalization; an integer in `[0,N)` selects a single-vertex impulse.
N must be positive. Zero factors, isolates, signed scores, and subnormal scores
are supported. The candidate need not be nonnegative or sum to one.

The source must already be **mathematically validated upstream**, including
binary memberships, retained factor cardinalities at least two, and exact
degrees `d_i = sum_{j in row_i}(cardinality_j - 1)`. Callers must bind that
validation to the source bytes and keep both inputs immutable throughout use.
The frozen manifest builder/identity validator provide the upstream mechanism;
this API intentionally does not rebuild or accept a manifest argument.

Framing is independently checked on both passes: matching headers, exact row
and score lengths, degree/count zero consistency, count limits, increasing
in-range factor IDs, total memberships, and EOF. Initial total lengths and the
necessary `2F <= Z <= NF` condition are checked before endpoint allocation.
These checks do **not** establish the degree/cardinality mathematics themselves.

Each pass hashes the exact source and score bytes it consumes. Different hashes
between passes cause refusal. Equal hashes detect ordinary inter-pass changes,
but are not locking, snapshots, adversarial concurrency protection, or a promise
about file contents after the call. SHA-256 collision resistance and immutable
code/inputs remain assumptions.

## Exact Target and Proof

Let B be the binary N-by-F incidence matrix, Q the diagonal of row membership
counts, and A = B B^T - Q. Repeated factors are allowed and add edge weight.
Upstream degree validation makes `d_i = sum_j A_ji`. Set `p_i=1/N` or the selected
impulse, and let P have columns `A_:j/d_j` for positive degrees and `p` for
dangling vertices. P is nonnegative and column-stochastic.

The target is the unique solution

```text
x* = (3/20) p + (17/20) P x*.
r  = (3/20) p + (17/20) P x - x.
```

For the exact real values of the serialized doubles, define

```text
y_i = x_i / d_i for d_i > 0, otherwise 0
g_j = sum_{i: B_ij=1} y_i
z   = sum_{i: d_i=0} x_i
(P x)_i = sum_{j: B_ij=1} g_j - q_i*y_i + p_i*z.
```

Pass one encloses g and z. Pass two encloses the last expression and each
residual coordinate. The upper bound R accumulates
`sum_i max(abs(residual_low_i), abs(residual_high_i))` upward. No coordinate is
sampled or omitted, including isolates and zero/negative scores.

Since `||P||_1=1`, the geometric resolvent gives

```text
||x - x*||_1 <= ||r||_1 / (1 - 17/20) <= (20/3) R.
```

This holds for arbitrary signed finite candidates; normalization is unnecessary.
It is the usual contraction/residual certificate, not a new theorem.

### Arithmetic Enclosure

For every binary64 addition, subtraction, multiplication, or division used in
the enclosure, compute a separate nearest/even operation and then use
`math.nextafter` toward the required infinity. One adjacent step brackets the
exact operation, including ties, cancellation, and rounding to zero under
gradual underflow. Both the operation result and the outward endpoint must be
finite; otherwise certification is refused. No relative-error-only model is
used, and negative intermediate endpoints are never clipped to zero.

Exact nonnegative integers are converted to a float and compared using its
exact `as_integer_ratio()`. If conversion rounded, the adjacent endpoint on the
other side of the integer is included. This covers uint64 degree/N values above
2^53. Exact 17/20 and 3/20 are enclosed separately by division; the latter is
not computed as `1 - float(17/20)`.

For interval division by a positive interval `[a,b]`, the lower denominator is
b for a nonnegative numerator endpoint and a for a negative endpoint. The upper
endpoint uses the converse choices. Multiplication by a nonnegative interval
also selects endpoints by sign. Thus negative sums are scaled in the correct
direction. Subtracting the self term uses crossed endpoints:

```text
[s_low, s_high] - [qy_low, qy_high]
    = [down(s_low - qy_high), up(s_high - qy_low)].
```

Induction over these operations gives the scatter, gather, residual, and L1
enclosures. O(1) scalar Decimal objects are used only for epsilon and the final
reporting/contraction bound, with an explicit ceiling Context. The binary64 R
is converted exactly before multiplying by 20 and dividing by 3 upward. There
are no Decimal or Fraction vectors and no Decimal work in membership loops.

### Runtime Admission

At entry and exit, the code checks radix, precision, and exponent limits and
executes live, non-constant-folded positive/negative halfway operations,
including an odd-mantissa tie. It also inspects result bits for subnormal
addition, multiplication, division, normal-to-subnormal transition, cancellation
to the minimum subnormal, and even-tie underflow. Bit checks avoid relying on
comparisons that a denormals-are-zero mode might itself alter.

The host must implement IEEE binary64 operations correctly and keep
nearest/even plus gradual underflow stable **throughout** certification. Entry
and exit probes are admission witnesses, not a proof of every hardware
instruction, and cannot detect a mode changed and restored in the middle.
Production never calls `fesetround`, changes FPCR/MXCSR, or changes the caller's
Decimal context. Mode mutation is confined to isolated test child processes.

## Results and Refusal Semantics

`accepted` is Boolean. `l1_error_upper` and `residual_l1_upper` are finite decimal
strings. `precision` must be an integer at least 20 and controls only final
Decimal bound rounding, not the binary64 endpoint width. Epsilon accepts a
finite positive built-in int, float, or Decimal, excluding bool. Acceptance is
the exact decimal comparison with `Decimal(str(epsilon))`, matching the frozen
reference's tolerance convention rather than converting the bound to float.

- `accepted=True`: the stated conditional error bound is at most epsilon.
- `accepted=False`: the computed bound exceeds epsilon; this is **not** proof
  that the candidate's true error exceeds epsilon.
- `ValueError`: malformed/changed input, invalid parameters, unsupported
  runtime, nonfinite score, operation overflow, or nonfinite outward endpoint.
  No infinity/NaN certificate or success-shaped fallback is returned.
- Ordinary I/O/allocation failures propagate. Neither input is written, on
  success or refusal. There is no file publication or rollback side effect.

Outward stepping at a largest finite endpoint may itself overflow even when
the rounded operation was finite; this deliberately refuses. Conversely, the
final Decimal error bound may exceed binary64's maximum while remaining a
valid finite decimal string. Signed zero and subnormal candidates are admitted.

## Resource Accounting

Returned counters on successful evaluation, including numerical non-acceptance:

| Quantity | Meaning |
| --- | --- |
| `retained_factor_payload_bytes = 16F` | Two length-F double endpoint payloads |
| `retained_decimal_vector_values = 0` | No persistent Decimal vector |
| `row_scans = 2`, `output_scans = 2` | Two complete passes over each input |
| `membership_visits = 2Z` | One scatter and one gather visit per membership |
| `logical_read_bytes = 2*source_bytes + 16N` | Bulk input payload only |
| `logical_write_bytes = 0` | Read-only API |

Logical read counters exclude the preliminary 32-byte header read, stat/EOF
probes, code-hash read, and Python imports. Input hashes are fused into the two
passes and do not add a third input scan. The implementation returns both
input SHA-256 identities and `certificate_code_sha256` for its on-disk module.
In-scan hashing, the final runtime guard, and the code-hash read are explicit
baseline protocol overhead, not required work of the residual theorem. A paired
timing report must disclose these differences from the frozen certificate.

**16F is selected logical payload, not peak RSS, physical RAM, or a 4 GiB fit
claim.** It excludes array object headers/capacity slack, allocation transients,
the full row tuple and temporary slices/bytes, Python integers and interpreter,
I/O buffers, scalar Decimal temporaries, hashing state, and OS page cache. This
reader has O(maximum row memberships) workspace and is not a bounded-memory
external importer. There is no allocation budget parameter in this API.

The integration driver's inspected call clears its exclusively owned h before
allocating these endpoints. Only with that ownership phase separation is the
selected factor peak 16F. Retaining a separate F-double h would make the
combined selected payload at least 24F, before all excluded memory. The direct
certificate cannot clear unrelated arrays retained by its caller. In a
publication pipeline its two certification scans are additional to publication
and upstream manifest validation/identity work; they are not total pipeline I/O.

## Verification Evidence

Environment: CPython 3.12.12, macOS 15.3.1, arm64. The shell's `python3` is
3.9.6, so the explicit interpreter below is necessary for the frozen helper's
`hashlib.file_digest` dependency. Run from the repository root:

```sh
/opt/homebrew/bin/python3.12 -B -m unittest discover \
  -s research_algorithms_20260920/experiments \
  -p test_binary64_rank_residual_certificate.py -v
```

TDD evidence: before the implementation existed, the original 16 tests failed
with `binary64 direct certificate is not implemented` (exit 1), including a
repeat under Python 3.12. After implementation, 15 passed and a publication
fixture failed: its handwritten h was wrong. The exact expanded oracle gave
`x=(20/43,20/43,3/43)`, hence `h=40/43`; deriving h from that oracle fixed the
fixture without changing the implementation. All 16 then passed. The final
focused suite has **17 passing tests, zero skips, exit 0**, including the added
exact tolerance-boundary check. No test duration is a public-data performance
measurement.

Coverage includes all 16 retained-factor subsets on three vertices with three
personalizations, 36 deterministic random perturbed graphs, complete expanded
Fraction residuals and exact stationary errors, repeated factors, signed
cancellation from minimum subnormals through 1e300, zero/isolated graphs,
integer denominators through `2^64-1`, interval sign combinations, overflow,
NaN/infinities, malformed headers/rows/IDs/outputs, second-pass mutation and
nonfinite scores, exact array allocations/counters/hashes, Decimal context
isolation, tolerance-boundary decisions, and frozen publication compatibility.

Stationary solutions use the frozen tiny expanded Fraction solver. Residuals
are independently expanded in the owned test file without using the tested
factor arithmetic. Rounding-mode and flush-to-zero refusal tests run in child
processes; the latter compiles a tiny temporary C shim. No repository file is
created by those children (`-B` prevents bytecode writes).

Separately, the lead reported **9 passing integration tests** after inspecting
this implementation: four paths with 12 complete outputs/audits, identical
binary-control versus frozen-direct output bytes, actual h release before
endpoint allocation, refusal preserving prior output, malformed workloads, and
structured failed-worker results. This is attributed integration evidence, not
a suite run by the owner of these three artifacts. The lead reported no blocker
and no public timing runs at that point.

## Remaining Limitations

1. No public-data timing, integration-driver benchmark, speedup, RSS, cold-I/O,
   durable-storage, or physical-memory-limit evidence was collected here.
2. The integration API/call site was inspected, the frozen publisher was checked
   on a tiny fixture, and the lead's integration results are recorded above;
   public-data sessions and performance interpretation remain with its owner.
3. Runtime negative-mode tests executed on macOS arm64 only. x86-64 child code
   is present but unexecuted here; unsupported platforms skip those witnesses.
4. No malicious concurrent-writer guarantee, manifest revalidation inside this
   function, or caller-state ownership enforcement is claimed.
5. Long sums and cancellation can widen intervals enough to refuse a good
   candidate. Raising `precision` cannot remove binary64 interval inflation;
   there is no Decimal retry hidden in this control.
6. Integer conversion kernels cover extreme uint64 denominators, but enormous
   real graphs were not materialized. Resource exhaustion is not bounded by 16F.
7. Tests and the enclosure argument are not machine-checked formal verification
   or an independent external code review.

## Exact Code Identities

SHA-256 of the exact files at completion, obtained with `LC_ALL=C shasum -a 256`.
The locale override avoids this machine's unsupported Perl `C.UTF-8` locale.

```text
ce919520838587b3fe966df5ff51f3692d02d954a9ee50605fcb795f65976e82  experiments/binary64_rank_residual_certificate.py
35a132f47700b285fe70570408b9c8d3c8e5fd3961678c5455cc6df268861973  experiments/test_binary64_rank_residual_certificate.py
d09558f63a906ccfdb859f7db6272caf1110474182b96fa7de088dfc3db64b65  experiments/probe_native_incidence_pagerank.py
022f3e8cea9ca870d7042bd2497818954705c85d71c93a27e5177e20d7de807a  experiments/streaming_rank_publication_certificate.py
d26e1e8900a85baf51881422cc1fe3fda4a3148aaecb758bdfc4edb65b2090d9  experiments/test_native_incidence_pagerank.py
```

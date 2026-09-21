# Precision-Placement Independent Review

Date: 2026-09-21. Scope: the new precision-placement publisher and its tests, the frozen scalar publisher, and section 10 of the publication note. Only this review file was authored. No implementation/test edits, commits, public-data runs, or public timings were performed. Small temporary synthetic graphs were used; their automatically returned timing fields were not used as performance evidence. No prior review portfolio was read.

## Verdict

**No certificate-soundness defect found under the declared binary64, immutable-source/state, and trusted-manifest contract.** The row-sum bound matches the primary theorem, outward propagation remains sound, and the shared frozen module is not mutated by this implementation. This is conditional code/proof review plus finite witnesses, not a general formal verification.

**Two actionable test findings remain**, prioritized below. In addition, the precision-placement tradeoff is real: exact-answer inputs can acquire different output bits and lose admission. More Decimal digits do not remove the binary64 row-bound floor. Correctness does not establish originality, speed, or physical RAM benefit.

## Prioritized Findings

### F1 [P2] The inherited live-mode child tests the frozen publisher, not the new one

Locations: [inherited test lines 197-225](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_streaming_rank_certificate.py:197), [new test lines 86-95](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_precision_placed_rank_certificate.py:86).

The inherited method calls the overridden loader but discards its result; its child literally imports `streaming_rank_publication_certificate` at line 206. Consequently its passing status in the new subclass is not live rounding-mode coverage of the new publisher. The new underflow test patches `validate_gradual_binary64_environment` to raise; it verifies call wiring, not that any real FTZ/DAZ mode is detected. Deleting the underflow probes while retaining the function call would evade these tests.

Action: make the child import the module under test, then test the real new entry point in directed modes and real denormal-flushing modes, preserving a preexisting output on refusal. Keep platform-specific capability checks/skips explicit. This review's separate Darwin ARM64 child passed nearest, upward, downward, toward-zero, and FZ-input/output checks; that evidence is **not** a committed regression test. Separate x86 FTZ-only and DAZ-only modes remain unexecuted.

### F2 [P3] The no-global-mutation assertion takes its snapshot after import

Location: [new test lines 69-84](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_precision_placed_rank_certificate.py:69), especially loading at line 70 and taking `original` at line 77.

A mutation made while executing the candidate module would already be present in `original`; line 84 would still pass. This matters because the actual extension point is installed at import time ([new lines 61-65](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/precision_placed_rank_certificate.py:61)).

Action: snapshot the frozen helper before loading the candidate; assert its identity immediately afterward and after publication. Also assert the private publisher's function globals and overridden row helper belong to that private instance. The review witness did these checks and found no actual shared-namespace mutation. This finding concerns regression sensitivity, not an observed implementation mutation.

## Mathematical And Code Checks

### Primary theorem and subnormal ufp

Primary source: Rump, [Error Estimation of Floating-Point Summation and Dot Product](https://www.tuhh.de/ti3/paper/rump/altRu11.pdf), pp. 1-6, especially Algorithm 3.2, Theorem 3.5 (3.11), and its following same-order warning. The PDF is the author-hosted accepted manuscript; its SHA-256 is recorded below.

The theorem bounds recursive signed summation by `(q-1) * 2^-53 * ufp(Ahat)`, where the absolute-value sum follows the **same order**. It permits gradual underflow, assumes no overflow, and has no summand-count restriction. Rounding must be nearest; the paper permits arbitrary tie handling, while this implementation deliberately requires nearest/even. These are the theorem's hypotheses, not properties supplied by increasing Decimal precision.

Application in [new lines 31-50](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/precision_placed_rank_certificate.py:31):

- Both accumulators advance in the same loop over the same `ids`. The initial addition to zero is numerically exact, so `count-1` is correct. Empty/singleton/all-zero cases are exact. Signed cancellation does not invalidate the use of the absolute accumulator.
- `frexp(Ahat)[1]-1` and `ldexp(1.0, exponent)` give the leading power of two for subnormals as well as normals. For the smallest nonzero accumulator the exponent is -1074, not the normal exponent-field formula -1023.
- The exact power is converted to Decimal before multiplication/division. Thus a radius below the smallest binary64 value is not accidentally rounded to zero. End checks reject overflow of **either** accumulator, including finite signed cancellation with overflowing absolute sum.
- The exact dyadic center, upward radius, downward subtraction, and upward addition enclose the exact real sum. Underflow-specific extra dot-product terms are not needed for this addition-only theorem.

### Runtime contract

[New lines 19-28](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/precision_placed_rank_certificate.py:19) calls the frozen runtime rounding probes, then checks raw result bits for subnormal input, output, and signed cancellation. Operands are created at runtime, not constant-folded expressions. These probes cover actual arithmetic rather than relying on `sys.float_info.rounds`.

The independent child used the local Darwin ARM64 SDK's `fenv_t` layout and FPCR constants. Under FZ, the bit pattern of `tiny+tiny` changed from 2 to 0, confirming that the bad mode was genuinely active; both the guard and new public publisher rejected it. All three directed rounding modes were also rejected. State was restored in `finally`, in a child process.

The guard is not a promise against a later foreign-library mode change or reassociation in a different implementation. Unchanged arithmetic throughout the call remains an explicit precondition. Direct callers of the internal sum helper must arrange that precondition themselves.

### Outward algebra and propagation

[New lines 53-58](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/precision_placed_rank_certificate.py:53) retains the exact integer denominator `20*d+17*q`; positive coefficients and denominator preserve interval order. Explicit floor/ceiling contexts are used even for negative endpoints. Large integer degrees are not first converted to binary64. At admitted precision >=20 the standard Decimal exponent range comfortably contains these binary64-derived values.

The unchanged frozen path then supplies the essential proof steps:

- [Frozen lines 198-220](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/streaming_rank_publication_certificate.py:198): the chosen actual `yhat` is charged its maximum endpoint distance; `(d+q)*delta` feeds the row ledger. The actual packed/unpacked output is charged against outward `d*[lo,hi]`. Any integer-to-float effect in `d*yhat` is consequently covered.
- [Frozen lines 224-239](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/streaming_rank_publication_certificate.py:224): `Rhat` encloses the weighted observed scatter/state difference; `Escatter` encloses the weighted scatter-rounding error; `Erow` covers row reconstruction uncertainty. The final charge is upward `17/3*(Rhat+Escatter+Erow)+Epub`.
- The exchange `sum_f s_f sum_(i in f) delta_i = sum_i (d_i+q_i)*delta_i` uses the manifest's exact degree/cardinality relation. Therefore the new row radius reaches the final bound; it is not merely reported and discarded.
- Isolates retain the exact effective-personalization enclosure and publication charge. The empty graph is refused by the underlying source contract; F=0 for a nonempty all-isolate graph is covered by inherited end-to-end cases.

No new bound gap was found in these steps. The inherited actual-file Fraction/original-residual checks do execute the new publisher via its overridden loader, except for the specific child in F1.

### Private reuse and ownership

The candidate executes the frozen file into a separate module dictionary, overrides only that instance's row helper, and calls a function whose globals point into the private dictionary. It does not patch the shared frozen module. The independent pre-import/post-call identity checks and Decimal global-context check passed.

This is not hermetic dependency isolation: both instances import the same `base` module, and the new runtime guard calls a shared frozen function. Cooperative immutable code/dependencies and exclusive ownership of h, source, and manifest are still required. The new source hash alone does not identify its runtime behavior if the frozen dependency changes; both hashes must accompany a receipt or experiment.

## Accuracy, Admission, And Stronger Controls

### Lower-endpoint bias is observable

The new interval feeds the inherited `yhat=float(lo)` choice ([frozen line 200](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/streaming_rank_publication_certificate.py:200)). That is valid but selects near one end of the uncertainty interval, not near its center. It can roughly double the interval-distance charge and move output bits even when the original row sum was exact. This is a design tradeoff, **not an unsound bound**; it does not imply every output is always below its exact target.

Witness: two vertices, q duplicate factors `[0,1]`, uniform personalization, exact dyadic `h_f=1/q`, and Rump scatter bounds for **both** publishers. The exact solution is `(0.5,0.5)`. At Decimal precision 50:

| q | Frozen final bound | New final bound | Exact L1 error of new bytes | New first score (hex) |
| --- | ---: | ---: | ---: | --- |
| 1 | 1.25825276124e-15 | 1.25825276124e-15 | 0 | 0x1.0000000000000p-1 |
| 2 | 1.25825276124e-15 | 4.51490696681e-15 | 1.11022302463e-16 | 0x1.fffffffffffffp-2 |
| 4 | 1.25825276124e-15 | 1.22864681392e-14 | 3.33066907388e-16 | 0x1.ffffffffffffdp-2 |
| 16 | 1.25825276124e-15 | 5.62883073485e-14 | 1.55431223448e-15 | 0x1.ffffffffffff2p-2 |
| 64 | 1.25825276124e-15 | 2.32295664186e-13 | 6.43929354283e-15 | 0x1.fffffffffffc6p-2 |

The frozen output remains exactly `(0.5,0.5)`. For q=2 and epsilon=2e-15, frozen accepts and new refuses, preserving the old file. The direct actual-output certifier bounds the new q=2 bytes by approximately 1.11022302463e-16 and gives zero for the frozen exact output.

Repeating at precision 20/50/100 leaves the output bits unchanged in this family. Precision 20 adds a small Decimal contribution; 50 and 100 agree at the displayed scale. Even q=1 retains the inherited conservative scatter floor despite zero actual error. More digits or more solver iterations cannot eliminate those particular certificate floors.

A second witness uses three vertices, two duplicate `[0,1]` factors, personalization at isolated vertex 2, and `h=[2^-100,-2^-100]`. At precision 100 the frozen path accepts epsilon=1e-20 with bound about 1.78808471850e-29 and emits `(-0.0,-0.0,1.0)`. The new row interval straddles zero, its lower-endpoint candidate is negative, and it raises the normal/nonnegative-scatter admission error. At precision 50 **both** paths refused in the exploratory run; this is not evidence of frozen acceptance at that precision.

Action for the next experiment: make these same-h/row-order, changed-bit, same-tolerance cases regressions. Test a central representative while retaining the full interval-distance charge; do not drop uncertainty or clip a claimed exact value. Compare any reserved higher-accuracy fallback or explicit refusal. The present behavior is allowed by section 10's stated admission tradeoff.

### Controls that remain necessary

Use the frozen **Rump** path, not only its weaker default gamma path, plus ordinary reconstruction/direct certification. Keep each solver's h and tolerance fixed. The outward-binary64 direct-residual comparator requested by section 10 was not implemented or measured in this review; neither were compensated/exact-sum alternatives. They remain open controls, not covered evidence.

A nonnegative-state specialization could avoid the second accumulator only after verifying its hypothesis; arbitrary signed CG coordinates cannot silently use it. The global row bound cannot generally be tightened using only the two existing aggregate sums without additional information; more accurate accumulation or an alternative certificate is a different control.

### O(N) is only the row-precision schedule

The new row kernel has constant-count Decimal operations per active row, removing the previous per-membership Decimal additions. The complete publisher still performs Decimal residual/scatter work per factor and output-ledger work per vertex: **O(N+F) high-precision operations**, versus O(Z+N+F) previously, at fixed precision. General binary64 and decoding work remains O(Z+N+F), including two row additions plus the scatter addition per membership.

The new receipt's `2*Z` counts row additions, not all floating-point work; its zero high-precision-membership count and active-row count are formula-derived, not instrumentation. F is not generally O(N): the two-vertex duplicate-factor family can increase F and Z while N stays fixed. Do not describe the whole certificate as O(N) precision work or infer a Z/N speedup.

Both scalar publishers retain the same modeled 16F-byte h/scatter payload, scalar Decimal state, and a full O(max q_i) decoded row. This change does not itself remove another F-sized vector. Builder/solver storage, Python objects, maximum row, allocation peaks, output buffers and page cache remain outside that payload claim. No RSS, whole-job RAM, 4-GB guarantee, or physical-memory improvement was measured.

## Test Evidence And Remaining Gaps

- Explicitly targeted suite: **24 tests passed**, comprising 14 new-subclass tests (including 10 inherited methods) and 10 frozen-class tests, with no skips on Darwin ARM64/Python 3.12.12. There are not 24 independent tests of the new implementation.
- The inherited complete-output test has 160 graph cases and three personalization choices, and exercises both scatter bounds. It checks actual emitted bytes against the expanded Fraction oracle and the original-residual certifier. Its normal new end-to-end runs use precision 50.
- Independent witness: 162 mixed-exponent/mantissa signed-sum cases, 156 with nonzero actual summation error, checked at precision 20/50/100 for both sum and reconstructed-row containment; separate subnormal and absolute/signed-overflow cases passed.
- The committed randomized sum cases use small integer multiples of one common power of two per vector. Their partial sums remain exactly representable at those sizes; they are not randomized roundoff stress. Fixed cancellation cases do help. Add mixed exponents/mantissas, ufp-boundary/order permutations, and row-level admission-floor comparisons.
- The pre-import isolation check, real new-module hardware-mode checks, precision sweep and changed-output/admission witnesses below were independent review commands, **not added tests**.
- Not established: separate x86 FTZ/DAZ coverage, arbitrary platform/compiler behavior, whole-call concurrent mode/source/state changes, wider lifecycle memory bounds, performance improvement, or scientific priority. Existing single-file atomic replacement and same-buffer provenance remain conditional on the frozen publisher's documented contract.

## Exact Reviewed Hashes

SHA-256, captured before review (dependencies when inspected) and rechecked before completion. All source/test paths below are inside `research_algorithms_20260920/experiments/`; the note is in its parent.

| File | SHA-256 |
| --- | --- |
| precision_placed_rank_certificate.py | `8cde5a642b189e74c5a32320b5e1aceafd6fd2ae6afe518249485a00ac90cd82` |
| test_precision_placed_rank_certificate.py | `6a16abd56154f8074763229818e781248c323f68521d052a99358158b17b6ab7` |
| streaming_rank_publication_certificate.py | `022f3e8cea9ca870d7042bd2497818954705c85d71c93a27e5177e20d7de807a` |
| test_streaming_rank_certificate.py | `d926d0aadd0227380d3cf67abde1b9eb548aa69b093c10172df5747c2d975dc8` |
| probe_native_incidence_pagerank.py | `d09558f63a906ccfdb859f7db6272caf1110474182b96fa7de088dfc3db64b65` |
| test_native_incidence_pagerank.py | `d26e1e8900a85baf51881422cc1fe3fda4a3148aaecb758bdfc4edb65b2090d9` |
| PageRank-Streaming-Publication-Certificate.md | `920a70fc0828262b47f119093ce1dab3fda5e9c3524cac757b24dc7f2a40f85e` |
| Rump primary PDF bytes | `4b23ffc2f7588ddc5218b914e520acf6c6c1ba0ec07bdae221f3d692fc254706` |

## Command Ledger

Commands below were actually executed. Source reading used scoped `nl`/`sed` and graph discovery; the graph lacked the new module, so its named source was read directly. Only the relevant base decoder/direct-certifier and Fraction oracle were inspected, not the surrounding portfolio.

| Command or attempt | Actual status |
| --- | --- |
| `git status --short` | Exit 0; research directory already untracked. |
| Initial `shasum -a 256 ...` without locale override | Exit 9: Perl rejected inherited C.UTF-8 locale. |
| `env LC_ALL=C LANG=C shasum -a 256 ...` | Exit 0; hashes above. |
| Web-reader open of primary PDF, twice | Both timed out; no theorem inference taken from failed fetches. |
| `curl -L --fail --max-time 60 https://www.tuhh.de/ti3/paper/rump/altRu11.pdf \| pdftotext -layout - -` | Exit 127: pdftotext absent; curl then reported write failure. |
| Primary-source command below | Exit 0; PDF extracted in memory and hashed. |
| Test command below using `python3` instead of the absolute 3.12 path | Exit 1: 24 tests, 16 errors and 2 failures from inherited missing `hashlib.file_digest`; 6 passed. This was an old-runtime failure, not evidence of a new arithmetic bug. |
| Test command below using Python 3.12 | Exit 0: 24 passed, no skips. |
| Exploratory in-memory Fraction/duplicate-factor/isolation and real-mode scripts | Exit 0; their substantive cases are retained in the consolidated witness below. The initial zero-row case refused in both modules at precision 50, then was refined to precision 100. |
| Consolidated independent witness below | Exit 0; `ALL INDEPENDENT WITNESSES PASS`. |
| ARM SDK `rg` including nonexistent `usr/include/arm/fenv.h` | Exit 2 for that missing path; `sed -n '124,185p' .../usr/include/fenv.h` then exited 0 and supplied the actual ARM64 layout/constants. |

Test/witness working directory:
`/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments`.

### Suite command

```sh
env PYTHONDONTWRITEBYTECODE=1 /opt/homebrew/bin/python3.12 -B -m unittest -v test_precision_placed_rank_certificate.PrecisionPlacedCertificateTests test_streaming_rank_certificate.StreamingCertificateTests
```

### Successful primary-source command

No local PDF or extraction file was created. The command printed the hash and first ten pages; mathematical review concerned pp. 1-6.

```sh
curl -sSL --fail --max-time 60 https://www.tuhh.de/ti3/paper/rump/altRu11.pdf | /Users/amuldotexe/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -B -c 'import sys,io,hashlib; from pypdf import PdfReader; data=sys.stdin.buffer.read(); print("PDF SHA256",hashlib.sha256(data).hexdigest()); r=PdfReader(io.BytesIO(data)); print("\n".join("PAGE "+str(i+1)+"\n"+p.extract_text() for i,p in enumerate(r.pages) if i<10))'
```

### Consolidated independent witness

This exact command ran successfully. It writes only disposable synthetic inputs/outputs inside `TemporaryDirectory`; `-B` and `PYTHONDONTWRITEBYTECODE` suppress bytecode files. Its real-mode portion is deliberately Darwin ARM64-specific and must not be reported as x86 coverage.

```sh
env PYTHONDONTWRITEBYTECODE=1 /opt/homebrew/bin/python3.12 -B - <<'PY'
import ctypes, importlib, math, platform, random, struct, subprocess, sys
from array import array
from decimal import Context, Decimal, ROUND_CEILING, ROUND_FLOOR, getcontext
from fractions import Fraction
from pathlib import Path
from tempfile import TemporaryDirectory
import streaming_rank_publication_certificate as frozen
import probe_native_incidence_pagerank as base
before = dict(vars(frozen))
decimal_before = str(getcontext())
import precision_placed_rank_certificate as new
assert before.keys() == vars(frozen).keys()
assert all(vars(frozen)[k] is v for k, v in before.items())
assert new._publisher is not frozen
assert new._publisher.publish_streaming_rank_certificate.__globals__ is vars(new._publisher)
assert new._publisher.enclose_reconstructed_row_value is new.enclose_reconstructed_row_value
new.validate_gradual_binary64_environment()
print("runtime", sys.version.split()[0], platform.system(), platform.machine())

rng = random.Random(20260921)
cases = []
for q in (2, 3, 9, 31, 257):
    for _ in range(30):
        bits = [(rng.randrange(2)<<63) | (rng.randrange(1924)<<52) | rng.getrandbits(52) for _ in range(q)]
        cases.append([struct.unpack("<d", struct.pack("<Q", b))[0] for b in bits])
for e in (-1022, -50, 0, 500):
    x = math.ldexp(1.0, e)
    cases += [[x, math.ulp(x)/2, -x],
              [math.nextafter(x, 0), math.ulp(x)/2, math.ulp(x)/2],
              [-x, math.nextafter(x, math.inf), -math.ulp(x)]]
nonzero = 0
for values in cases:
    exact, rounded = sum(map(Fraction, values)), 0.0
    for value in values:
        rounded += value
    nonzero += Fraction(rounded) != exact
    h = array("d", values)
    for p in (20, 50, 100):
        lower, upper = Context(prec=p, rounding=ROUND_FLOOR), Context(prec=p, rounding=ROUND_CEILING)
        lo, hi = new.enclose_binary64_signed_sum(h, range(len(h)), lower, upper)
        assert Fraction(lo) <= exact <= Fraction(hi)
        lo, hi = new.enclose_reconstructed_row_value(h, range(len(h)), 2**64-1,
                                                      Decimal(".15"), Decimal(".15"), lower, upper)
        y = (3+17*exact)/(20*(2**64-1)+17*len(h))
        assert Fraction(lo) <= y <= Fraction(hi)
print("Fraction mixed-exponent cases", len(cases), "nonzero errors", nonzero, "PASS")
for p in (20, 50, 100):
    lower, upper = Context(prec=p, rounding=ROUND_FLOOR), Context(prec=p, rounding=ROUND_CEILING)
    for e in (-1074, -1073, -1023, -1022):
        v = math.ldexp(1.0, e)
        lo, hi = new.enclose_binary64_signed_sum(array("d", [v, v, -v]), range(3), lower, upper)
        assert Fraction(lo) <= Fraction(v) <= Fraction(hi)
for values in ([sys.float_info.max, -sys.float_info.max], [sys.float_info.max]*2):
    try:
        new.enclose_binary64_signed_sum(array("d", values), range(2), lower, upper)
    except ValueError as error:
        assert "overflow" in str(error)
    else:
        raise AssertionError("overflow admitted")
print("subnormal and signed/absolute overflow witnesses PASS")

with TemporaryDirectory() as directory:
    store, meta, out = [Path(directory)/s for s in ("rows", "meta", "out")]
    for q in (1, 2, 4, 16, 64):
        base.build_incidence_row_store(store, 2, [[0, 1]]*q)
        manifest = new.build_certificate_source_manifest(store, meta)
        h = array("d", [1/q]*q)
        for p in (20, 50, 100):
            records = []
            for module in (frozen, new):
                r = module.publish_streaming_rank_certificate(store, meta, manifest, h, out, None, 1.0,
                                                               precision=p, summation_bound="rump")
                xs = struct.unpack("<2d", out.read_bytes())
                actual = sum(abs(Fraction(x)-Fraction(1, 2)) for x in xs)
                assert actual <= Fraction(r["l1_error_upper"])
                direct = base.certify_published_rank_output(store, out, None, 1.0, precision=p)
                records.append((float(r["l1_error_upper"]), float(actual),
                                float(direct["l1_error_upper"]), xs[0].hex()))
            print("DUP", q, p, "frozen/new", records)
        if q == 2:
            for module in (frozen, new):
                out.write_bytes(b"prior")
                r = module.publish_streaming_rank_certificate(store, meta, manifest, h, out, None, 2e-15,
                                                               summation_bound="rump")
                assert r["accepted"] == (module is frozen)
                assert r["accepted"] or out.read_bytes() == b"prior"
                print("epsilon 2e-15", module.__name__, r["accepted"])
    base.build_incidence_row_store(store, 3, [[0, 1], [0, 1]])
    manifest = new.build_certificate_source_manifest(store, meta)
    for module in (frozen, new):
        out.write_bytes(b"prior")
        try:
            r = module.publish_streaming_rank_certificate(store, meta, manifest, array("d", [2**-100, -2**-100]),
                                                           out, 2, 1e-20, precision=100, summation_bound="rump")
            assert module is frozen and r["accepted"]
            assert struct.unpack("<3d", out.read_bytes()) == (0.0, 0.0, 1.0)
            print("zero-row", module.__name__, r["l1_error_upper"])
        except ValueError as error:
            assert module is new and "scatter" in str(error) and out.read_bytes() == b"prior"
            print("zero-row", module.__name__, str(error))
assert all(vars(frozen)[k] is v for k, v in before.items())
assert str(getcontext()) == decimal_before
print("import/call namespace isolation and Decimal global context PASS")

program = r'''
import ctypes, platform, struct
from array import array
from pathlib import Path
from tempfile import TemporaryDirectory
import precision_placed_rank_certificate as module
import probe_native_incidence_pagerank as base
assert platform.system() == "Darwin" and platform.machine() == "arm64"
class Fenv(ctypes.Structure):
    _fields_ = [("fpsr", ctypes.c_ulonglong), ("fpcr", ctypes.c_ulonglong)]
lib = ctypes.CDLL(None)
lib.fegetenv.argtypes = [ctypes.POINTER(Fenv)]
lib.fesetenv.argtypes = [ctypes.POINTER(Fenv)]
old = Fenv()
assert lib.fegetenv(ctypes.byref(old)) == 0
with TemporaryDirectory() as directory:
    store, meta, out = [Path(directory)/s for s in ("rows", "meta", "out")]
    base.build_incidence_row_store(store, 2, [[0, 1]])
    manifest = module.build_certificate_source_manifest(store, meta)
    try:
        for label, mode in [("nearest", 0), ("up", 0x400000), ("down", 0x800000),
                            ("zero", 0xc00000), ("FZ", 0x1000000)]:
            out.write_bytes(b"prior")
            env = Fenv(old.fpsr, (old.fpcr & ~0x1c00000) | mode)
            assert lib.fesetenv(ctypes.byref(env)) == 0
            current = Fenv()
            assert lib.fegetenv(ctypes.byref(current)) == 0
            assert current.fpcr & 0x1c00000 == mode
            tiny = struct.unpack("<d", struct.pack("<Q", 1))[0]
            tiny_bits = struct.unpack("<Q", struct.pack("<d", tiny+tiny))[0]
            try:
                module.validate_gradual_binary64_environment()
                guard = "accepted"
            except ValueError as error:
                guard = str(error)
            try:
                r = module.publish_streaming_rank_certificate(store, meta, manifest, array("d", [1.0]),
                                                               out, None, 1e-10)
                publisher = "accepted" if r["accepted"] else "bound refusal"
            except ValueError as error:
                publisher = str(error)
            assert lib.fesetenv(ctypes.byref(old)) == 0
            if mode:
                assert guard != "accepted" and publisher != "accepted" and out.read_bytes() == b"prior"
            else:
                assert guard == publisher == "accepted" and tiny_bits == 2
            if label == "FZ":
                assert tiny_bits == 0
            print("MODE", label, guard, publisher)
    finally:
        assert lib.fesetenv(ctypes.byref(old)) == 0
'''
result = subprocess.run([sys.executable, "-B", "-c", program], capture_output=True, text=True)
print(result.stdout, end="")
assert result.returncode == 0, result.stderr
print("ALL INDEPENDENT WITNESSES PASS")
PY
```

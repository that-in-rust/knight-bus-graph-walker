# Streaming Certificate Code Audit

Date: 2026-09-21. Independent, narrow implementation audit. Fixes belong to the lead. Only this review document was added; the frozen base probe was not edited.

## Findings First

### 1. [P1] The arithmetic guard does not detect a changed runtime rounding mode

Location: [streaming_rank_publication_certificate.py:144](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/streaming_rank_publication_certificate.py:144), scatter addition at line 195; gamma and Rump evaluation at lines 94-115.

`sys.float_info.rounds` is not a live check of the thread's floating-point environment. On this Darwin arm64 host, changing the runtime mode to upward left it equal to 1, so the admission guard still passed. Both bounds then underestimated an actual sequential positive-normal sum. A native extension changing the mode is enough; no malformed graph is required to expose the arithmetic precondition gap. This is not a counterexample to either theorem under its stated nearest-rounding assumption.

Executed scalar witness: sum `1.0` followed by 99 copies of `2**-60` under upward rounding. Weighted actual scatter error was approximately `2.1896547075517248e-12`; gamma reported `1.1102230246252056e-12`, Rump `1.099120794378905e-12`. These are error magnitudes, not timings. I did not construct a full graph whose final certificate underbounds its answer; the witness establishes failure of a ledger component while the implementation's mode guard admits the environment.

```python
# Darwin arm64 only: FE_UPWARD is 0x400000. Run in a disposable process.
import ctypes
import sys
from fractions import Fraction as F
import streaming_rank_publication_certificate as s

lib = ctypes.CDLL(None)
old = lib.fegetround()
try:
    assert lib.fesetround(0x400000) == 0
    assert sys.float_info.rounds == 1
    z = 1.0
    for _ in range(99):
        z += 2.0**-60
    actual = 100 * abs(F(z) - (F(1) + 99 * F(1, 2**60)))
    assert actual > F(s.bound_positive_scatter_error(100, z, 50))
    assert actual > F(s.bound_rump_scatter_error(100, z, 50))
finally:
    assert lib.fesetround(old) == 0
```

Lead action: establish/check the actual arithmetic environment, or explicitly make it an externally enforced precondition instead of presenting this check as verification. Add an isolated-process test to the admission tests, currently [test_streaming_rank_certificate.py:142](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_streaming_rank_certificate.py:142). Restore the mode on every test exit.

### 2. [P1] Both direct publication modes can replace the source or metadata

Location: [benchmark_rank_publication_comparison.py:62](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/benchmark_rank_publication_comparison.py:62), atomic replacement at line 101.

Unlike the streaming publisher's lines 138-139, the direct wrapper never rejects output/input path aliasing. With two vertices, one factor `{0,1}`, `h=[1.0]`, and `output=store`, both ordinary direct and `same_output=True` returned `accepted=True` and replaced the 64-byte incidence source with 16 bytes encoding `(0.5,0.5)`. Setting output to metadata has the same destructive replacement path. The normal driver's fresh output directory avoids this, but the callable wrapper does not protect its source contract.

Reproducer, executed separately for both modes:

```python
from array import array
from pathlib import Path
from tempfile import TemporaryDirectory
import probe_native_incidence_pagerank as b
import streaming_rank_publication_certificate as s
import benchmark_rank_publication_comparison as c

for same in (False, True):
    with TemporaryDirectory() as tmp:
        store, metadata = Path(tmp)/"rows", Path(tmp)/"metadata"
        b.build_incidence_row_store(store, 2, [[0, 1]])
        manifest = s.build_certificate_source_manifest(store, metadata)
        result = c.publish_direct_rank_certificate(
            store, metadata, manifest, array("d", [1.0]),
            store, None, 1e-10, same_output=same)
        assert result["accepted"] and store.stat().st_size == 16
```

Lead action: apply the streaming path exclusions before any direct output work. Add source, metadata, and resolved-symlink alias cases to [test_rank_publication_comparison.py:22](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_rank_publication_comparison.py:22), asserting unchanged inputs.

### 3. [P2] Exceptional refusals abort the benchmark and discard completed observations

Location: [benchmark_rank_publication_comparison.py:170](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/benchmark_rank_publication_comparison.py:170), driver lines 199-217.

`subprocess.run(check=True)` propagates worker errors, and the driver writes its receipt only after every triplet. Numerical admission errors, malformed inputs, and audit exceptions therefore bypass the `all_accepted=False` handling. A failure after successful triplets leaves no receipt containing those observations or the refusal cost. This contradicts the receipt's `all outcomes retained` protocol and prevents accounting for exception-based refusals, not just ordinary excessive-bound refusals.

Executed a control-flow-only probe: stub preparation; return three successful worker dictionaries; raise `CalledProcessError` on the fourth call. One completed triplet was printed, the exception escaped, and `receipt.exists()` was false. No subprocess or benchmark was run. The `1.0` values below are synthetic branch-control sentinels, never performance evidence.

```python
import subprocess
from unittest.mock import patch

# c, Path, TemporaryDirectory imported as in the previous probe.
with TemporaryDirectory() as tmp:
    folder = Path(tmp)
    receipt = folder/"receipt.json"
    argv = ["probe", "--store", str(folder/"unused"), "--folder", str(folder),
            "--receipt", str(receipt), "--repeats", "2"]
    ok = {"total_session_ms": 1.0, "all_accepted": True}
    calls = [ok, ok, ok, subprocess.CalledProcessError(1, ["worker"])]
    with patch.object(c.sys, "argv", argv), \
         patch.object(c, "prepare_shared_factor_states", return_value={}), \
         patch.object(c, "execute_publication_worker_session", side_effect=calls):
        try:
            c.main_publication_comparison_experiment()
        except subprocess.CalledProcessError:
            assert not receipt.exists()
        else:
            raise AssertionError("expected propagated worker failure")
```

Lead action: retain structured failed-worker outcomes and already completed triplets, record elapsed failure work, and suppress comparison ratios for failed admissions. Add this driver-level test; the existing comparison tests cover only normal sessions and truncated state files.

### 4. [P2] An empty or shortened preparation silently changes the benchmark workload

Location: [benchmark_rank_publication_comparison.py:122](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/benchmark_rank_publication_comparison.py:122), unchecked iteration at line 128 and vacuous `all(...)` at line 160.

The session does not require exactly the three declared personalization queries. A valid store plus `preparation.json` containing `{"manifest": manifest, "states": []}` returns `queries=[]`, `total_stage_ms=0`, and `all_accepted=True` for all four methods. It does not even reach metadata identity validation. A shortened list similarly executes a smaller workload without declaring the missing queries. The normal preparer creates three states, but the session consumes serialized input without enforcing that invariant.

Small probe: build the two-vertex fixture above with metadata named `certificate-metadata.bin`; write the empty preparation object to a temporary `preparation.json`; call `run_publication_stage_session(store, folder, folder, method)`. I executed this for `direct`, `direct_same`, `stream`, and `stream_rump`; all reported success with zero queries. No worker processes were launched and no timing values are evidence here.

Lead action: validate workload length, ordered sources `(None, 0, n-1)`, and required state fields before timed publication. For `n=1`, repeated source zero is intentional; do not require unique sources. The test's assertion at [test_rank_publication_comparison.py:29](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_rank_publication_comparison.py:29) checks the producer, not this malformed-consumer case.

### 5. [P2] Receipt construction can fail after the old answer has been replaced

Location: [streaming_rank_publication_certificate.py:232](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/streaming_rank_publication_certificate.py:232), subsequent filesystem calls at lines 242-244. The direct wrapper has the same order at lines 100-110.

After `os.replace`, constructing the returned receipt still calls `store.stat()` and `metadata.stat()`. An ordinary reporting I/O failure therefore raises without returning the in-memory certificate, even though the prior answer has already been replaced. This is a narrow exception-safety issue in the existing API, not a demand for a persisted two-file transaction or crash durability. The numerical bound had passed; this is not a numerically rejected candidate being published.

Executed fault injection on a valid two-vertex source with `h=[1.0]`: start with `output=b"prior"`; patch `Path.stat` to raise only for the source after the output changes. The API raised `OSError("receipt stat failure")`, and the prior output was no longer present.

```python
# Within the valid two-vertex temporary fixture, with output initially b"prior":
from unittest.mock import patch
real_stat = Path.stat
def failing_receipt_stat(path, *args, **kwargs):
    if path == store and output.read_bytes() != b"prior":
        raise OSError("receipt stat failure")
    return real_stat(path, *args, **kwargs)

with patch.object(Path, "stat", failing_receipt_stat):
    # Raises after replacing output, instead of returning its certificate.
    s.publish_streaming_rank_certificate(
        store, metadata, manifest, array("d", [1.0]), output, None, 1e-10)
```

Lead action: obtain fallible size/provenance data and construct the substantive receipt before replacement. Keep the final commit-to-return path minimal. Add the case alongside [test_streaming_rank_certificate.py:98](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/test_streaming_rank_certificate.py:98), distinguishing pre-commit refusal from a post-commit reporting error.

## Additional Test Gaps

- **Serialized-value correspondence:** the updated streaming lines 206-213 decode the actual encoded buffer before computing Epub. I injected a packer returning zero bytes while retaining the real decoder on a one-isolate graph. The current code correctly returned `accepted=False`, bound `1`, and preserved the prior output. Add this as a regression test. The existing damaged-file test at lines 95-96 damages an already completed output; it does not exercise encoder-to-ledger correspondence. No current serialization defect is claimed.
- **I/O refusal:** the current tests do not inject a short score write, buffered-close failure, replacement failure, or a truncated candidate before the complete-length check. For each, assert the old output survives and no temporary file remains. Current ordering/checks appear appropriate for these pre-commit failures, but they are not tested by the ordinary tolerance-refusal case.
- **Malformed source and metadata:** the builder correctly delegates sorted/distinct/in-range IDs, row counts, trailing-data and nonempty-universe validation to the frozen reader; it independently checks factor sizes and exact degrees. Existing new tests exercise changed source identity, a bad degree, and metadata truncation, but omit duplicate IDs, out-of-range IDs, membership-count mismatch, retained singleton factors, invalid metadata magic/dimensions, and an invalid selected isolate byte. Use small fixtures and sentinel outputs. To reach semantic header checks rather than only SHA mismatch, deliberately update the supplied metadata digest in a clearly labeled malformed-fixture test.
- **Trust boundary, not a new defect:** query-time hash checks do not establish the semantic correctness of a self-consistently forged source/metadata/receipt. They assume the trusted builder and unchanged snapshot, as the updated contract explicitly states. Forged isolate counts, flags, or cardinalities with recomputed hashes must not be described as validated real sources. Concurrent source or h mutation is also outside the declared contract.
- **Directed arithmetic extremes:** six selected tests passed, but the full-output test uses default precision and small degrees. Twelve additional in-memory Fraction checks passed at precision 20/50, ambient Decimal precision 6, cancellation states, and synthetic degrees `3`, `2**53+1`, `2**64-1`. The row enclosure and distance contained their exact rational targets. These synthetic scalar checks do not establish realizable source metadata. Add such focused regressions plus full-publisher subnormal/overflow refusal cases; current admission tests mostly call the scalar validator directly.

## Benchmark Accounting

The new `direct_same` control genuinely uses the same row reconstruction and precision as the streaming path in the inspected code. Both candidate modes are explicit; the reference solver/certifier remains unchanged. Setup, upstream CG-to-h mapping, state reads and identity hashes are not free: the driver reports preparation separately and includes loading/identity work in session time. Do not promote `total_stage_ms`, which omits state loading, into end-to-end time.

The direct actual-file audit is explicitly outside candidate timing and the pre-audit RSS snapshot at benchmark lines 147-160. That is a legitimate certificate-stage experiment if labeled as such, but not measured whole-worker/whole-job memory or time: the streaming worker subsequently allocates the reference Decimal vectors. `child_wall_ms_including_audit` includes that work; there is no corresponding post-audit whole-worker RSS field. Source-row decoding, builder arrays, Python object overhead and allocator capacity remain outside the advertised `16F` array payload figure. No lower physical-RAM conclusion follows from it.

**[P3] Byte counters are traversal approximations, not exact logical-read totals.** For a successful streaming call with immutable files, line 244 omits the initial 32-byte source-header read and the separate 72-byte metadata-header read: its formula is short by 104 bytes. The builder's line 62 similarly omits its initial 32-byte header. The direct formula at lines 109-110 is short by 136 bytes when including the frozen reference certifier's own initial header read. These are static counts of successful `read` bytes, not disk-I/O measurements; they do not establish a meaningful performance difference. Either include these reads at the wrapper/reporting layer without changing the frozen base probe, or name the counters as modeled bulk traversal bytes. Preparation JSON and per-session header traffic also need explicit accounting if an exact session total is claimed.

The failed-outcome retention defect above is more important than these small counter discrepancies: it can erase the expensive refusal itself. No speedup, RSS improvement, or public timing conclusion is offered by this audit.

## Verification And Snapshot

The implementation changed during review. Final findings and line references were refreshed against the gamma/Rump, decoded-emitted-value, and `direct_same` version. I read the exact publication contract and the completed mathematical-review sidecar; I did not repeat theorem or primary-literature research.

Final inspected SHA-256 values:

| File | SHA-256 |
| --- | --- |
| `experiments/streaming_rank_publication_certificate.py` | `3aacc0e5c31652a5ac24203425cd2278011adb2eba798e6ba24d7f9d8ef58e66` |
| `experiments/benchmark_rank_publication_comparison.py` | `f00621667c6384d15026f36aff2913b69e35430da55d2e4e8e392b6944de52aa` |
| `experiments/test_streaming_rank_certificate.py` | `372adc46fb5a28c06b83dbccfd546f2ac2a3f57b1f4059cf257c166c7bcb065b` |
| `experiments/test_rank_publication_comparison.py` | `383b1a5b103b54ecef522132178080cc3dea65f0e7162676df86f1da487229d2` |
| `experiments/probe_native_incidence_pagerank.py` (frozen) | `d09558f63a906ccfdb859f7db6272caf1110474182b96fa7de088dfc3db64b65` |
| `PageRank-Streaming-Publication-Certificate.md` | `8eb34adaec35e84bbf4ed7bdc297d9038d6b1ac39d02813b0f90fe787c27d53a` |

Executed on Homebrew Python 3.12 with bytecode writing disabled: `test_arbitrary_states_preserve_bound`, `test_refusal_preserves_existing_output`, `test_manifest_validates_source_degrees`, `test_normal_positive_arithmetic_admission`, `test_rump_control_exact_bounds`, and `test_stronger_bound_changes_admission`. All six passed on the refreshed implementation. I did not run the exhaustive 960-output test, the benchmark-session test, broader suites, upstream solves, or public benchmark workers. Reproducer imports assume the experiments directory is on `PYTHONPATH`; all filesystem probes used automatically removed temporary directories outside the repository.

Audit handoff: all small probes and selected tests have exited, temporary fixtures were removed, and the runtime-rounding probe restored its original mode before exit. No audit subprocess, synthetic worker, or background compute remains. The lead can proceed with fixes and its separately controlled public timing phase without overlap from this audit.

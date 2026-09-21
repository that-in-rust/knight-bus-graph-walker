# Boolean PageRank: Actual-Byte Admission And Strong Structural Controls

Date: 2026-09-21. Implemented, checked and measured under the scope below.
This is part of the seven-family research, not completion of that goal.

## Research Question

Can we turn the proved mass-deflated error bound into a useful complete-output
publication path, without promoting a result that is matched or beaten by a
simpler exact solver for the same graph family?

The preceding [81-attempt study](PageRank-Boolean-Streaming-Evidence.md) found
a precise failure: at the largest binary64 damping below one, accurate answers
were refused by the generic residual certificate. The frozen F and P answers
had exact-oracle L1 errors below 1e-15, but residual/(1-alpha) bounds near 0.45
and 0.50. Those old refusals remain refusals. Their output was not published.

This follow-on separates three questions:

1. Can an implemented directed mass/gap bound certify the actual staged bytes?
2. Does it improve complete query cost and state, not only a scalar formula?
3. What remains after adding the exact same-source direct-target control?

Question 3 is essential. Uniform all-pair symmetry gives an ordinary closed-form
inverse. A comparison only against iterative CG would conceal that alternative.
Neither stationary-mode deflation nor a known symmetry inverse is claimed as a
new generic mathematical primitive. The contribution question is a useful,
precisely scoped complete architecture, not a new name for either operation.

## Exact Scope

Every original vertex belongs to exactly two distinct groups. Every canonical
pair of F groups occurs, with the same positive multiplicity h. There are no
other classes or isolates. Shared membership creates one Boolean undirected
edge between distinct vertices, even if both memberships overlap.

```text
P = F(F-1)/2 classes, F >= 4
n = hP original vertices
d = h(2F-3)-1 neighbors per vertex
lambda_bar = [h(F-3)-1] / d < 1/2
```

All finite nonnegative binary64 personalization weights are interpreted exactly
before normalization. They need not be equal across or within classes; their
total must be positive. The implementation admits only degrees/heights supported
by the existing exact-integer-to-binary64 lift contract. These restrictions are
checked, not inferred from favorable output or a sampled eigenvalue.

The source builder and its pinned read transaction are trusted. Canonical class
validation does not authenticate an arbitrary malicious database or prove that
a customer's claimed graph equals the source. Raw edge-list conversion into
such memberships, and finding useful naturally eligible datasets, remain open.

## Implemented Options

```text
             PINNED, VALIDATED SOURCE
                        |
            +-----------+-----------+
            |           |           |
         F-space CG  P-space CG  Exact group inverse
            |           |           |
            +-----------+-----------+
                        |
              Staged original-ID bytes
                        |
         +--------------+---------------+
         |              |               |
      Generic       Mass-deflated   Exact-target
      residual      residual        comparison
         |              |               |
         +--------------+---------------+
                        |
           Accepted, bound/hash/snapshot match?
                  |                 |
                 yes                no
                  |                 |
         No-replace publication   Cleanup + refusal
```

The separate `experiments/boolean_rank_certified_options.py` exposes all nine
solver/certificate combinations for this source family. The old generic
publisher and five files used in the original measurement remain untouched.

### Mass-Deflated Actual-Byte Certificate

For actual staged scores x, let s=sum(x), pi=1/n, and
r=(1-alpha)*p+alpha*T*x-x. Then rho=(1-alpha)*(1-s), and

```text
V = n * sum_i r_i^2 - rho^2
error_L1 <= abs(s-1) + sqrt(V)/(1-alpha*lambda_bar).
```

Implementation in `experiments/boolean_rank_deflated_certificate.py`:

- Validate topology in a paid canonical class scan.
- Open the candidate once. First pass accumulates mass, F interval group
  totals, and P sequential class-total scratch records.
- Second pass uses those totals to enclose the original Boolean residual,
  including the pair-overlap and self-edge subtractions.
- Use an upper bound on squared residual norm and a **lower** bound on rho^2
  when subtracting. Reject contradictory negative variance; do not clip it.
- Bound sqrt upward by the context successor of Decimal's correctly rounded
  square root. Enclose the denominator downward and final error upward.
- Match both complete hashes, record counts, original-ID order and snapshot.
  Close nested iterators and remove scratch on success, refusal or exception.

The mass term cannot be omitted: x=pi/2 has zero centered residual variance
but L1 error 1/2. A guessed gap cannot be substituted for the proved gap.
The [independent mathematical review](PageRank-Boolean-Mass-Deflation-Review.md)
and [original derivation](PageRank-Boolean-Mass-Deflation.md) retain these
counterexamples and primary-source attribution.

### Stronger Exact-Target Control

The [direct control](PageRank-Boolean-Uniform-Pair-Control.md) retains F exact
group statistics, rather than n scores or P class scores. The all-pair inverse
reduces to a stationary part and F-1 group-contrast coordinates. It needs no
iterative matrix application. Its emitted binary64 scores still require checking.

For certification, it independently reconstructs the exact group coordinates,
then obtains each true vertex score from an exact global weight coefficient
and current-class offset. It sums the exact distance from the actual binary64
output. It therefore pays one output pass and zero P scratch, without any
residual amplification. Fresh certification pays validation, group accumulation
and class/vertex comparison; it does not reuse rounded solver state.

Exact Fraction arithmetic is not free. The receipt includes F rational slots,
aggregate numerator/denominator bit peaks, and exact target/error scalar bit
peaks. Scalar temporaries, Python objects, SQLite buffers and runtime overhead
are additional. F slots do not mean 8F bytes or a physical RAM cap.

### Publication Contract

The option publisher validates scalar parameters and topology before solving,
writes the full candidate privately, flushes/fsyncs it, and releases solver
arrays before certification. It publishes by no-replace hard link only after
acceptance and matching SHA-256, count and snapshot. Existing files and files
created by a racing writer are preserved. Refusal carries the complete
certificate/cost receipt but removes the private candidate.

The original source remains caller-owned and reusable. This is not a claim of
directory crash durability or security against a writer capable of mutating
private files concurrently. There is no public partially verified result.

## Work And Storage Boundaries

| Phase/option | Explicit retained state or work | What is not implied |
| --- | --- | --- |
| Shared preflight | One P-class scan, constant metadata | No free reuse of prior validation |
| F CG | Packed F solver coordinates and streamed classes | Not a universal solver-speed or RSS improvement |
| P CG | Packed P coordinates plus F centered operator state | Not the old cancellation-broken comparator |
| Direct inverse | F exact rational state, then F returned doubles; 2P source records | Not constant-width integer arithmetic |
| Deflated certificate | 2F Decimal endpoints; 2n output records; P scratch records; scalar counters | Not O(1) total storage or physical RAM |
| Exact-target certificate | F rational state; n output records; zero scratch; 3P source records | Not necessarily lower CPU than directed Decimal |
| Result delivery | n complete original-ID records, 16n bytes | No top-k-only shortcut or omitted zero-score IDs |
| Prepared source | SQLite vertices, groups, classes and indexes | No free import, refresh, B-tree work or OS cache |

Numbers of scalar slots, file bytes, output reads and observed maximum RSS are
reported separately. No SQLite cache setting is described as a process cap.
The prior source-construction receipt is reused and linked, not rerun or added
to query peak RSS as if simultaneous. A first-job cost includes preparation;
a later job can amortize it only while the source remains suitable and current.

## Verification Before Timing

The lead wrote eight publisher tests first and observed eight missing-module
failures. They passed after implementation; two file-backed/race tests then
passed. The driver similarly had three missing-module failures before green.

Final lead replay before timing: all 113 source/solver/certificate/publisher/
driver/control tests pass with warnings treated as errors. The publisher
covers 18 normal solver/certificate combinations and
nine near-one attempts, exact-oracle full answers, tight-error refusal, invalid
options before scans, mismatched hash/count/snapshot, weak-reference proof of
solver-array release, source cursor cleanup and no-replace publication races.

The independent mathematical checker was read and replayed: 13 exact spectra,
936 candidate inequalities, 1,872 interval checks and 1,016 Decimal checks.
Its finite oracle is independent of this implementation; its review explicitly
does not audit production byte plumbing. The [separate implementation
audit](PageRank-Boolean-Deflation-Code-Review.md) is terminal. Its 723 exact
actual-byte enclosure checks, 24 malformed-byte refusals, 16 admission failures,
11 injected lifecycle failures and nine SQLite failure/reuse probes were
lead-replayed. It identified a consumed-prefix mutation hazard: matching hashes
do not attest that previously read bytes stayed unchanged. The lead reproduced
the issue, added a failing regression, then initial/final descriptor metadata
checks; the regression and adapted checker now reject the demonstrated write.
Exclusive ownership of staged bytes remains necessary. Metadata checks are not
an adversarial immutability proof. The lead patch is not a second independent
audit; the numerical checker was replayed unchanged. Both agents and all
correctness commands were terminal before the serial experiment began.

## Predeclared Experiment

Driver: `experiments/measure_boolean_rank_options.py`.

- Reuse exactly `pairs-small`, `pairs-medium`, `pairs-wide`, `near-one` from
  `evidence/boolean-rank-sqlite-20260921/`. Verify original database and five
  original implementation hashes before running. Preserve original receipts.
- Six modes: F/generic, F/deflated, P/deflated, direct/generic, direct/deflated,
  and direct/exact-target. All nine API combinations are correctness-tested;
  this six-mode portfolio is the declared timing scope.
- For each of five noncontrol modes and each case, run three fresh-process
  direct-exact/candidate/direct-exact brackets: 180 attempts, 60 brackets.
- Execute workers serially, after agent correctness work is terminal. Do not
  clear the OS cache or call fresh processes cold-cache measurements.
- Primary interval starts before source open and ends after source close.
  Phase timings include preflight, solve, full output, certificate and publish.
  Process-launch wall time is separate. Fresh source-build cost is not included
  in these repeated-query times; its earlier receipt remains explicitly paid.
- Retain failures, complete certificate-refusal receipts, all raw timings and
  process maximum RSS. Admit a bracket to stable timing summaries only when
  both control durations have max/min <=1.5. Keep unstable data as data.
- Audit every accepted candidate against both control outputs using exact
  binary64 L1 differences bounded by the sum of certified errors. Hash every
  admitted output; no output may exist for a refusal.
- Hash all ten participating implementation modules before/after the run and
  recheck the four frozen source databases. Save full events and final receipt.

No conclusion will promote synthetic family eligibility, output acceptance or
a scan-count reduction into a universal Neo4j speed/RAM claim. A direct-target
implementation can also be optimized with directed approximate arithmetic;
the exact-rational comparator here is structurally strong, not a proof of the
fastest possible machine implementation.

## Results And Decision

The fresh serial study completed with exit 0: **180 attempts, 174 admitted
complete outputs, six refusals, 60 control brackets.** All 54 fully admitted
brackets pass the predeclared stability screen and both exact paired-output
audits. The admitted files contain 554,424 original-ID scores, 8,870,784 bytes.
No refused output exists, and no worker retains a source cursor. Peak live
source cursors are two. The earlier 81-attempt receipt is unchanged.

Artifacts:

- [Complete receipt](evidence/boolean-rank-options-20260921/receipt.json), SHA-256
  `9a9ed4852b51cee6102c22433e8672a011be31518db350b705dd1942ab1cae6f`.
- [All worker events](evidence/boolean-rank-options-20260921/events.jsonl), SHA-256
  `326ace8354e9fabe138b57bd00444aaca88f03c55f4bed7f07d923c4dd10861d`.
- All 174 admitted `.ranks` files are retained in the same directory. The
  input databases stay in the earlier evidence directory; this is source reuse,
  not a second uncharged copy.

Runtime: Python 3.11.15 on macOS 15.3.1 arm64. Fresh workers do not imply an
empty filesystem cache. Three candidate observations per case/mode are useful
local paired evidence, not population estimates or confidence intervals.

### Complete Query Time

Medians in milliseconds, source-open through source-close. Candidate cells
contain three executions; the direct/exact column contains 30 bracket controls
per case. Refusal durations are shown but cannot compete as delivered answers.

| Case; n / F / P | F/generic | F/deflated | P/deflated | Direct/generic | Direct/deflated | Direct/exact |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Small; 112 / 8 / 28 | 3.23 | 3.29 | 3.59 | 3.11 | 3.14 | 2.94 |
| Medium; 4,032 / 64 / 2,016 | 94.45 | 100.21 | 115.11 | 87.63 | 93.19 | 97.60 |
| Wide; 8,128 / 128 / 8,128 | 306.80 | 318.48 | 388.74 | 275.84 | 294.20 | 312.44 |
| Near-one; 56 / 8 / 28 | Refused, 2.67 | 2.89 | 3.32 | Refused, 2.47 | 2.68 | 2.48 |

The primary paired comparison divides each candidate duration by the mean of
its immediately surrounding direct/exact controls, then takes the median of
three ratios. Below, negative percentages mean less time than those controls.
They are not ratios of the rounded aggregate medians in the previous table.

| Case | F/generic delta | F/deflated delta | P/deflated delta | Direct/generic delta | Direct/deflated delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| Small | +9.97% | +13.02% | +22.05% | +4.96% | +5.61% |
| Medium | -3.46% | +2.27% | +17.71% | -10.51% | -5.40% |
| Wide | -1.27% | +1.53% | +23.91% | -12.28% | -6.74% |
| Near-one | No answer | +15.74% | +33.59% | No answer | +7.84% |

The result is a portfolio, not a universal winner. At ordinary alpha=0.85,
direct/generic is the fastest measured medium/wide complete plan. Paying for
the stronger deflated certificate there is unnecessary for epsilon=1e-10.
At near-one alpha, direct/exact is the fastest admitted measured plan. It
removes the need to rely on residual amplification for this symmetric family.
F/deflated enables a valid publication where F/generic refuses, but does not
beat the stronger source-specific control on this fixture.

### Admission Is Real, Not A Better-Looking Scalar

The six refusals are three F/generic and three direct/generic near-one attempts.
Every other attempt is admitted, including all near-one F/P/direct deflated
outputs and all 30 direct/exact controls for that case.

| Near-one solver | Generic L1 bound for its actual output | Deflated L1 bound |
| --- | ---: | ---: |
| F CG | 0.4507207940 | 1.4097849144e-16 |
| P CG | 0.4995932314, diagnostic from deflated check | 9.3262077516e-16 |
| Direct | 0.4995932314 | 1.5546465792e-16 |

All target epsilons are 1e-10. The new spectral denominator for this case is
about 0.64; the generic denominator is about 1.11e-16. These are bounds on
actual staged bytes and include mass error. They do not change the original
PageRank operator or silently loosen tolerance. The exact-target control
reports an exact rational error, not a potentially loose residual bound.

### Scratch, Reads And Observed RAM

Direct/exact uses zero class scratch for every admitted output, versus about
3.9 KB, 295 KB and 1.145 MB on the small/medium/wide deflated runs. It reads
16n actual output bytes instead of 32n. Those are **100% less class scratch
and 50% fewer certificate output-read bytes**, not 100% less storage or 50%
less complete-query I/O. Original source scans, result writes and validation
still occur. The direct/exact certificate rereads 3P class records.

For the wide case the result file is 130,048 bytes and the reused source is
577,536 bytes; the approximately 1.145 MB deflated scratch is larger than
either. Removing it is a real owned-file reduction, but not a measured general
RAM breakthrough. Python Fraction arithmetic helps explain why the scratch-free
exact-target certificate is slower on the two larger cached cases than the
Decimal residual alternatives: its wide certificate median is 201.59 ms,
versus 182.39 ms for direct/deflated and 165.05 ms for direct/generic.
This is a plausible mechanism supported by phase timings, not a profiler result.

Whole-worker maximum RSS ranges overlap:

| Mode | Minimum / maximum observed MiB across its attempts |
| --- | ---: |
| F/generic | 20.80 / 22.19 |
| F/deflated | 20.81 / 21.08 |
| P/deflated | 20.81 / 21.59 |
| Direct/generic | 20.81 / 21.28 |
| Direct/deflated | 20.81 / 21.88 |
| Direct/exact | 20.70 / 22.36 |

There is no demonstrated large or consistent whole-process RAM saving. These
small fixtures and cached scans cannot establish a 4/5/10 GB hard-budget claim.
Reported RSS does not turn filesystem cache, arithmetic temporaries or SQLite
cache configuration into controlled memory. Ten participating source hashes
remain stable across the experiment; the five original hashes still match the
old receipt, and all four input database hashes match their original builds.

### Preparation Remains Paid

Earlier build receipts for the same byte-identical sources, not new timing runs:

| Case | Recorded build elapsed ms | Retained database bytes |
| --- | ---: | ---: |
| Small | 3.99 | 28,672 |
| Medium | 30.56 | 237,568 |
| Wide | 74.52 | 577,536 |
| Near-one | 3.82 | 28,672 |

These numbers are native-membership preparation, not discovery from arbitrary
expanded edge lists. Adding them to a later median query gives only an
illustrative accounting estimate, not a freshly observed cold end-to-end time.
Updates, new snapshots, disk capacity and concurrent jobs need separate study.

### Decision

1. Retain the directed certificate as an explicit research option, not the
   generic default and not a new PageRank theorem.
2. Include the direct inverse and exact-target comparison whenever this exact
   uniform topology is admitted. Iterative-only baselines are now incomplete.
3. Treat direct/generic versus direct/exact as an observed time/scratch/admission
   tradeoff, with no inferred hard-RAM or latency guarantee.
4. Broaden the next mathematical question to unequal class sizes, where the
   uniform inverse no longer applies. The [heterogeneous gap derivation](PageRank-Boolean-Heterogeneous-Gap.md)
   has 83 exact whole-space checks and a false-gap negative control, but no
   independent review or integrated publisher yet. Keep the weighted variance
   and actual-byte boundary explicit; do not merely swap a scalar bound.
5. Keep all seven algorithm-family contribution gates open. This result closes
   a concrete implementation/comparator question, not the requested portfolio.

## Retained Integrity Check

Run the sole Python block from the repository root. It is an offline artifact
audit, not a memory-bounded execution path. It rechecks all input/code/output
hashes, every original ID, all 108 admitted pair comparisons, all refusal paths
and event/receipt agreement. No performance timing is taken.

```python
from fractions import Fraction
import json
import math
from pathlib import Path
import struct
import sys

base = Path('research_algorithms_20260920')
sys.path.insert(0, str(base/'experiments'))
from boolean_rank_sqlite_source import SqliteBooleanRankSource
from measure_boolean_rank_workflow import hash_measurement_file_bytes as digest, audit_boolean_output_pair

root = base/'evidence/boolean-rank-options-20260921'
receipt = json.loads((root/'receipt.json').read_text())
prior_path = Path(receipt['prior_receipt'])
assert digest(prior_path) == receipt['prior_receipt_sha256']
for name, value in receipt['source_hashes'].items():
    assert digest(base/'experiments'/name) == value
universes = {}
for prep in receipt['reused_preparations']:
    path = prior_path.parent/prep['database']
    assert digest(path) == prep['database_sha256']
    with SqliteBooleanRankSource(path) as source:
        universes[prep['case']['name']] = [v for cid,_,_,_,_ in source.iterate_class_records()
                                         for v,_ in source.iterate_class_vertex_rows(cid)]
        assert source.events['active_cursors'] == 0
events = [json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
assert events == receipt['queries'] and len(events) == 180
expected_files = {'receipt.json','events.jsonl'}
admitted = refused = rows = 0
for index, query in enumerate(events):
    assert query['trial'] == index and query['source_events']['active_cursors'] == 0
    path = root/query['output']
    if not query['accepted']:
        assert not path.exists()
        assert not query['pipeline']['certificate']['accepted']
        assert Fraction(query['pipeline']['certificate']['l1_error_upper']) > Fraction(query['pipeline']['epsilon'])
        refused += 1
        continue
    expected_files.add(path.name)
    pipeline = query['pipeline']
    assert digest(path) == pipeline['output_sha256'] == pipeline['certificate']['output_sha256']
    records = list(struct.iter_unpack('<Qd',path.read_bytes()))
    assert [v for v,_ in records] == universes[query['case']]
    assert all(math.isfinite(x) and x >= 0 for _,x in records)
    assert len(records) == pipeline['output_rows'] == pipeline['certificate']['nrows']
    assert path.stat().st_size == 16*len(records) == pipeline['output_bytes']
    rows += len(records)
    admitted += 1
pair_checks = 0
for bracket in receipt['brackets']:
    if not bracket['all_accepted']:
        continue
    first,candidate,last = [events[i] for i in bracket['trials']]
    for label,control in (('before',first),('after',last)):
        bound = sum(Fraction(q['pipeline']['certificate']['l1_error_upper']) for q in (candidate,control))
        actual = audit_boolean_output_pair(root/candidate['output'],root/control['output'],bound)
        assert actual == bracket['audit_'+label]
        pair_checks += 1
assert {p.name for p in root.iterdir()} == expected_files
assert (admitted,refused,rows,pair_checks) == (174,6,554424,108)
print('Integrity: 10 code hashes, 4 sources, 180 events, 174 outputs, 6 absences, 108 pair audits')
print('Complete original-ID rows:',rows,'bytes:',16*rows)
print('Receipt SHA-256:',digest(root/'receipt.json'))
print('Events SHA-256:',digest(root/'events.jsonl'))
```

## Remaining Seven-Family Work

This study closes a concrete A01 publication question. It does not establish
seven new graph algorithms, customer-native prevalence, physical RAM budgets,
or publication novelty. If the direct control dominates this restricted family,
retain it as the appropriate compiled plan and move the research question to
sources where a justified useful gap exists but an equally cheap exact-target
formula does not. Do not weaken the baseline to preserve a favorable claim.

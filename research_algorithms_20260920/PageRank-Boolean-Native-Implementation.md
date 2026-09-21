# Native Boolean PageRank: Paid Gap Certificates On Incomplete Pair Sources

Date: 2026-09-21. Research implementation and completed predeclared comparison.
This is A01 progress within the seven-family goal, not completion of that goal.

## Question And Scope

Can a gap certificate that avoids an expanded Boolean-overlap graph make
complete original-record PageRank answers more useful on missing-pair,
unequal-multiplicity sources? Does factor-space solving retain an advantage
over an equally informed class-space solver after preparation, output and
certification are included?

This is not arbitrary node-edge PageRank. Records have two memberships;
distinct records are adjacent once when either membership overlaps. An input
transaction becomes an output vertex, not a conventional account vertex.
The comparison cannot support a claim about a production Neo4j query without
first establishing that this operator is the requested query.

## Implementation Contract

- `experiments/boolean_rank_native_gap_source.py`: two matching class passes,
  checked original degrees, union-by-size tree construction and exact tree
  moments. F>=3, connected, no unused groups or singleton/empty memberships.
- `experiments/boolean_rank_native_certificate.py`: directed degree-weighted
  actual-byte residual calculation for the original operator. Two complete
  output reads and one P-class scratch stream. No auxiliary-walk substitution.
- `experiments/boolean_rank_native_pipeline.py`: staged original-ID output,
  file fsync, solver arrays released before certification, no-replace hard
  link only after acceptance, typed refusal with an inspectable receipt.
- `experiments/measure_boolean_rank_native.py`: fresh serial preparation/query
  workers, full cost receipts and every refused attempt retained.

The native publisher pays two preflight class passes, and its standalone
verifier independently pays another two validation passes. It does not use a
trusted cached-gap shortcut. Generic controls pay neither native preflight
nor native gap construction. Both plans still pay their actual solve, full
output, certification and publication costs.

These are prototype Python implementations over a trusted pinned SQLite
source. There is no physical RAM enforcement, hostile-database security
claim, directory crash-durability guarantee, or independent code audit of
these new modules. The ten earlier uniform-study modules remain unchanged.

## Review-Driven Refinement

The independent [native theorem review](PageRank-Boolean-Native-Gap-Review.md)
accepted the mathematical transfer and F=3 complete-pair extension, not a
novelty claim or production implementation. Its runnable checker replayed:
63 independent fixtures, 170 tree certificates, 13 eligibility refusals.

The review also tightened the transfer. Define M as groups incident to at
least two distinct pair classes, and let N_med=min_{a in M} N_a. Only those
groups mediate off-diagonal class transitions, so

```text
E_G >= 2*N_med*E_R
delta_G >= min(1, N_med*delta_H/d_max).
```

Connected H on F>=3 has a nonempty M. This improves or equals the original
N_min comparison without changing the requested walk. One F-counter array
is additionally charged during the first pass; it is released with the
union-find arrays before tree moments. The original unrefined bound remains
in the receipt for an explicit ablation. A unit five-edge star improves
from 5/26 to 25/26. This is an elementary refinement, not new canonical-path
theory. The original Boolean graph for a star is a clique, which also has
an exact closed-form PageRank solution; do not present winning against CG
there as an algorithmic breakthrough.

## Predeclared Experiment

All cases have unequal multiplicities, incomplete pair coverage and positive
nonuniform personalization. Each runs at alpha=0.85 and the binary64 value
immediately below one, epsilon=1e-10. Precision=60, CG tolerance=1e-13,
maximum iterations=1000, SQLite cache setting=256 KiB. That cache setting
is not a physical RAM limit.

| Source shape | Groups F | Multiplicity base | Purpose |
|---|---:|---:|---|
| Cycle | 16 | 4 | Small connected, missing-pair baseline |
| Path | 64 | 4 | Bottleneck and weak gap adversary |
| Chorded cycle | 64 | 4 | Sparse but more connected group source |
| Star | 64 | 4 | Mediating-group refinement stress; clique caveat |
| Dense incomplete pairs | 96 | 2 | P substantially larger than F |

The generator deterministically varies each present pair height over one,
two or three times the base. It visits all F-choose-2 potential pairs; that
generation cost belongs to build time. There is no claim of O(P)-time input
generation. Source preparation, input bytes, full 16n-byte output, temporary
certificate bytes and process peak RSS are separately reported.

Four modes: factor-generic, factor-native, class-generic, class-native.
Class-native is the equally informed control. For each of the other three
modes and each case, run three control/candidate/control brackets. Thus the
planned study is ten preparations, 270 query attempts and 90 brackets.
All execution is serial. Process startup is recorded separately from query
time; fresh workers do not imply cold filesystem cache. Before/after control
max/min <=1.5 is the predeclared timing stability screen, with all observations
retained. No tail-percentile or confidence-interval claim from three repeats.

Every all-admitted bracket is audited with exact rational arithmetic over
both complete original-ID outputs and the sum of certified error bounds.
This is a consistency check, not an independent ground-truth PageRank solve.
Unit tests separately compare small outputs with an expanded exact solve.
Refused output is absent and must never enter a speedup-to-answer calculation.
Implementation hashes and prepared source hashes must match before/after.

## State Before Measurement

Publisher tests first had five missing-module failures, then passed. The leaf
refinement test first observed the old 5/26 result instead of 25/26, then
passed. Four measurement-contract tests first failed for the absent driver,
then passed. The native source/certificate/publication/measurement suite now
contains 24 tests. These include 144 seeded exact original-answer comparisons
inside the standalone certificate tests, malformed output and cursor cleanup.

Measurement results will be appended only after the serial worker process
terminates and the evidence files can be checked. Neither low RAM nor improved
latency has yet been established for this native extension.

The paragraph above records the pre-measurement state. The following results
supersede that state without changing the declared experiment.

## Completed Study

The serial driver exited zero. Ten preparations and 270 query attempts
produced **201 admitted complete outputs and 69 explicit certificate
refusals**. All failures were `NativeRankCertificateRefusal`, not an omitted
exception or partial publication. The outputs contain 510,528 original-ID
scores, totaling 8,168,448 bytes. Of 90 brackets, 54 admitted all three outputs;
53 of those passed the predeclared stability screen. Each of the 54 has two
exact complete-output consistency audits, including the unstable bracket.

Artifacts live in `evidence/boolean-rank-native-20260921/`:

```text
receipt.json SHA256
ae7a2cf8804780f85217a9c082f9fed5df5f6a7ef659bec41dbc81d4d71e7330
events.jsonl SHA256
793f274ce8293b378ee61a12f9b88226fe9bc992ee76cf15e372569c4246b912
```

### Complete Query Results

Median milliseconds include opening the prepared source, preflight where
applicable, solving, complete output, certification, publication and source
close. They exclude separately reported preparation and process startup.
F denotes factor CG; P denotes class CG. An `R` entry gives the time spent
**refusing**, not time to an answer. The control has 18 observations per row;
each other mode has three. These small samples do not support tail estimates.

| Case | n / P / F | F generic | F native gap | P generic | P native gap |
|---|---|---:|---:|---:|---:|
| Cycle, 0.85 | 124 / 16 / 16 | 3.36 | 3.84 | 4.17 | 4.33 |
| Cycle, near one | same | R 5.25 | 5.35 | R 4.74 | 5.42 |
| Path, 0.85 | 504 / 63 / 64 | 11.59 | 12.59 | 16.42 | 17.52 |
| Path, near one | same | R 29.43 | 30.90 | R 54.12 | 55.17 |
| Chorded, 0.85 | 1,536 / 192 / 64 | 25.91 | 28.63 | 38.22 | 41.01 |
| Chorded, near one | same | R 42.34 | R 45.32 | R 91.83 | R 95.29 |
| Star, 0.85 | 504 / 63 / 64 | 8.10 | 9.17 | 9.22 | 10.22 |
| Star, near one | same | R 8.22 | 9.24 | R 10.77 | 12.00 |
| Dense incomplete, 0.85 | 13,824 / 3,456 / 96 | 238.31 | 282.86 | 360.12 | 400.35 |
| Dense incomplete, near one | same | R 301.97 | 344.77 | R 650.29 | R 689.42 |

For ordinary damping, native-versus-native locally paired F query reductions
are 10.81%, 27.95%, 30.26%, 10.00% and 30.64% respectively. But **F generic
is the fastest measured ordinary-damping mode on every shape**. Adding a
gap calculation unconditionally is not justified by those results. Also,
the star operator is a clique and has a stronger unmeasured closed-form
control; its iterative comparison is a stress result, not a best-plan claim.

For near-one damping, both native CG routes admit cycle/path/star; only F
native admits dense; neither admits chorded. The native F reductions over
native P on stable all-admitted brackets are 4.59% (cycle, two stable brackets),
44.01% (path) and 23.33% (star). No speed-to-answer ratio is defined against
a refused control. The unstable cycle bracket had control max/min=1.9403;
it remains in the raw evidence but is excluded from the stable comparison.

The entire set of 270 child queries consumed about 40.12 seconds of summed
process wall time. Median process-wall minus query time was about 29.19 ms;
that difference includes interpreter/import cost, receipt handling and the
post-query output hash, not solely OS spawn latency. Millisecond kernel
comparisons should not be sold as command-line end-to-end latency.

### Preparation, Storage And RAM

| Shape | Build time, normal / near (ms) | Prepared DB bytes | Original Boolean edges | Class certificate scratch bytes |
|---|---:|---:|---:|---:|
| Cycle | 4.56 / 4.37 | 28,672 | 1,370 | 2,214 |
| Path | 6.28 / 6.35 | 49,152 | 5,700 | 8,831 |
| Chorded | 11.68 / 11.85 | 86,016 | 68,640 | 27,538 |
| Star | 6.55 / 6.42 | 49,152 | 126,756 | 8,999 |
| Dense incomplete | 85.57 / 85.92 | 622,592 | 3,942,144 | 486,186 |

Postbuild gap inspection is separately recorded and not free: roughly 0.4 ms
for the small cycle and 20.2 ms for dense. Query-side preflight and validation
are charged again. The preparation measurement does not instrument peak
temporary filesystem occupancy. Scratch in the table is the logical class
file size for these runs, not total temporary disk or peak host RAM.

Across all query workers, observed peak RSS spans **20.86-24.38 MiB**, with
heavily overlapping mode medians. No complete-process RAM win is established.
In the dense case, conservative solver-vector payload is 13,824 bytes for F
versus 336,384 for P, a 95.89% reduction of **that selected component**, about
315 KiB in absolute terms. The source, Python runtime, Decimal certificate,
buffers and output remain. Converting that ratio into a claim of 95.89% less
product RAM would be false.

The dense-normal F-native median components are approximately 20.40 ms
preflight, 49.25 ms solve, 49.55 ms full output, and 162.11 ms certificate.
Thus repeated source validation and certification are major costs after
the reduced solve. Its certificate reads 442,368 output bytes and writes
and rereads 486,186 scratch bytes. No result delivery was omitted.

## Failure Analysis And Research Decision

1. **A valid gap can still be too weak.** The chorded gap is
   6144/22789609. At near-one damping the F error upper bound is about
   1.9603e-10 and the P bound 2.6251e-10, both above 1e-10. This is a refusal
   by the implemented bound, not proof that the actual answers are wrong.
2. **The two solvers do not have identical numerical output.** Dense near-one
   gives a bound about 1.2600e-11 for F but 1.4332e-10 for P. Do not attribute
   this admission difference purely to state size or exact-arithmetic CG.
3. **Internal convergence flags are not the correctness contract.** Sixty-six
   admitted near-one outputs have `solver.converged=false`. Their reduced
   residual tests become unhelpful at extreme conditioning, while the
   original actual-byte error certificate admits them. Their receipts retain
   both facts. Solver flags are not silently changed to true.
4. **The stationary distribution is a mandatory stronger next control.**
   Near alpha=1, directly trying pi_v=d_v/vol_G with a certified error bound
   may avoid CG altogether. For exact pi, the standard reversible bound is
   `(1-alpha)*||p-pi||_(pi^-1)/[(1-alpha)+alpha*delta_G]`; actual binary64
   output also incurs rounding error. This control is not implemented or
   timed here. It is established reasoning, not another claimed innovation.
5. **The input semantics remain restrictive.** These tests rank original
   two-membership records, not arbitrary vertices in arbitrary directed
   graphs. A real native source must justify that overlap query and charge
   its export/build/update costs before product conclusions follow.
6. **Paper novelty remains open.** The reviewed auxiliary walk, spectral
   comparison and path-congestion ingredients have direct precedent. An
   implementable source-to-output certificate is useful evidence, but the
   present result does not establish an arXiv-worthy new graph algorithm.

Decision: retain F/generic as the ordinary-damping measured choice among
these four plans, and native-gap as an optional admission tool. Do not claim
a RAM win or make the gap plan mandatory. Do not pursue more decimal-precision
variants before checking stationary/clique controls and real source semantics.
The seven-family research objective remains active; this advances A01 without
promoting it beyond the evidence or replacing work on the other six families.

Subsequent follow-through: the [direct controls](PageRank-Native-Direct-Controls.md)
are now implemented in separate files and measured on these same frozen
databases. Stationary output admits all tested near-one shapes, including
chorded, and beats CG where both admit. Exact clique output also beats the
ordinary star CG plan. Those new results supersede an iterative-only choice,
not the historical numbers above. The 270-attempt receipt and its eleven
implementation files remain byte-for-byte unchanged.

## Reproduce And Check

Run the study in a new directory, not over the retained evidence:

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B measure_boolean_rank_native.py \
  --directory ../evidence/boolean-rank-native-new-run
```

This integrity checker runs from the repository root. It validates retained
hashes, event/receipt agreement, refusal absence, all complete generated IDs,
and the exact paired-output audits. It is not a new algorithmic oracle or a
code audit. The small exact-oracle tests are a separate evidence layer.

```python
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import math
import struct
import sys

root = Path('research_algorithms_20260920')
sys.path.insert(0, str(root/'experiments'))
from measure_boolean_rank_workflow import audit_boolean_output_pair
evidence = root/'evidence/boolean-rank-native-20260921'
def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()
assert digest(evidence/'receipt.json') == 'ae7a2cf8804780f85217a9c082f9fed5df5f6a7ef659bec41dbc81d4d71e7330'
assert digest(evidence/'events.jsonl') == '793f274ce8293b378ee61a12f9b88226fe9bc992ee76cf15e372569c4246b912'
data = json.loads((evidence/'receipt.json').read_text())
events = [json.loads(line) for line in (evidence/'events.jsonl').read_text().splitlines()]
assert [x for x in events if x['kind']=='preparation'] == data['preparations']
assert [x for x in events if x['kind']=='query'] == data['queries']
for name, expected in data['source_hashes'].items():
    assert digest(root/'experiments'/name) == expected, name
for prepared in data['preparations']:
    assert digest(evidence/prepared['database']) == prepared['database_sha256']
queries = {q['trial']: q for q in data['queries']}
assert len(queries) == 270 and len(data['preparations']) == 10
rows = output_bytes = admitted = 0
for query in queries.values():
    path = evidence/query['output']
    assert query['source_events']['active_cursors'] == 0
    assert query['accepted'] == query['pipeline']['certificate']['accepted']
    if not query['accepted']:
        assert query['failure_type'] == 'NativeRankCertificateRefusal'
        assert not path.exists()
        continue
    receipt = query['pipeline']
    assert digest(path) == receipt['output_sha256'] == query['postprocess_output_sha256']
    assert receipt['output_sha256'] == receipt['certificate']['output_sha256']
    assert path.stat().st_size == receipt['output_bytes'] == 16*receipt['output_rows']
    assert Fraction(receipt['certificate']['l1_error_upper']) <= Fraction(receipt['epsilon'])
    count = 0
    with path.open('rb') as stream:
        while record := stream.read(16):
            identifier, score = struct.unpack('<Qd', record)
            assert identifier == count and math.isfinite(score) and score >= 0
            count += 1
    assert count == receipt['output_rows'] == receipt['certificate']['nrows']
    rows += count
    output_bytes += path.stat().st_size
    admitted += 1
audits = stable = all_admitted = 0
for bracket in data['brackets']:
    before, candidate, after = [queries[t] for t in bracket['trials']]
    assert bracket['all_accepted'] == all(q['accepted'] for q in (before,candidate,after))
    ratio = max(before['query_seconds'],after['query_seconds'])/min(before['query_seconds'],after['query_seconds'])
    assert bracket['control_time_ratio'] == ratio
    assert bracket['timing_stable'] == (ratio <= 1.5)
    if bracket['all_accepted']:
        all_admitted += 1
        stable += bracket['timing_stable']
        for label, control in (('before',before),('after',after)):
            bound = sum(Fraction(q['pipeline']['certificate']['l1_error_upper']) for q in (candidate,control))
            assert audit_boolean_output_pair(evidence/candidate['output'], evidence/control['output'], bound) == bracket['audit_'+label]
            audits += 1
assert (admitted,rows,output_bytes,audits,all_admitted,stable) == (201,510528,8168448,108,54,53)
print('Native evidence verified:', admitted, 'outputs;', rows, 'rows;', audits, 'pair audits')
print('Ten source hashes and eleven implementation hashes match; all 69 refusals retained.')
```

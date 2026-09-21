# Native PageRank: Stationary And Clique Controls Before Another Solver

Date: 2026-09-21. Reviewed derivation, tested implementation and completed
comparison, predeclared below before its execution.
This follows the completed native Boolean study; it does not alter its frozen
270-attempt evidence or establish new PageRank mathematics.

## Why These Controls Are Necessary

At alpha immediately below one, the prior iterative candidates approach the
ordinary random-walk stationary distribution. It is unfair to imply that CG
is indispensable for a fixed absolute-error contract. Conversely, replacing
PageRank with degree centrality without an error proof is incorrect for
general personalization and ordinary damping.

Three choices are therefore meaningful: retain CG plus original residual
certification; try stationary output with a paid bias certificate; or, on a
proved clique, use the exact closed form. The latter two will be explicit
controls, not automatically substituted answers or claimed innovations.

## Stationary Candidate And Original-Answer Bound

Let W=A D^-1 for the undirected connected original Boolean graph, with no
isolates, pi_v=d_v/vol, personalization p=w/sum(w), and a in [0,1).
Let delta>0 be a proved lower bound on the algebraic normalized-Laplacian gap.
Then on the zero-mass subspace, reversibility gives

```text
x* - pi = (1-a) (I-aW)^-1 (p-pi)
||x* - pi||_1 <= B
B = (1-a)*sqrt(K)/[(1-a)+a*delta]
K = sum_v p_v^2/pi_v - 1
  = vol/sum(w)^2 * sum_v w_v^2/d_v - 1.
```

The weighted inverse norm is at most the inverse denominator; Cauchy-Schwarz
transfers it to L1 because sum(pi)=1. Negative eigenvalues, including -1 on
bipartite graphs, do not invalidate the algebraic resolvent argument.
For an arbitrary actual nonnegative finite output y, not merely an assumed
rounding of pi, the valid bound is `||y-x*||_1 <= ||y-pi||_1+B`.

The verifier will read every actual output ID/value, accumulate an upward
bound for distance from pi, and accumulate the weighted personalization
second moment with directed arithmetic. It will not assume that the writer
actually emitted pi. The bound includes representation error before admitting
an answer. A positive gap and a small (1-a) do not guarantee admission for
every p, precision, epsilon or source.

## Exact Clique Candidate

After validated source degrees, check every class has d=n-1. For the simple
Boolean graph this proves a clique without enumerating edges. The exact
PageRank value is

```text
x*_v = [(1-a)*(n-1)*p_v + a]/(n-1+a).
```

Compute each value independently. Certify actual-byte distance from that exact
target in one read, not by a generic residual amplified by 1/(1-a). This is
ordinary algebra, including for personalized scores. A unit star input in
group space is a clique in record space; these objects must not be confused.

## Retained-State And Lifecycle Contract

Use the existing trusted pinned native-source validator: two paid P-class
passes and O(F) tree/counter state, released before the output phase. These
controls deliberately do not claim a better physical cap or skip validation.
Write a complete original-ID candidate to a private file, fsync, then run an
independent verifier which repeats its own source validation. Admit via a
no-replace hard link only after hash, row count and snapshot agreement.

The candidate and verifier keep constant scalar state after validation, no
rank vector and no P-class scratch file. The full 16n-byte output is mandatory.
The verifier reads it once. Directed Decimal contexts have explicit precision
and exponents; exact source totals and local fractions still have bit costs.
Class sums are accumulated before division where appropriate. Runtime,
source cache, buffering, builder state and bit widths are not physical-free.

Required tests: exact dense answers on small connected irregular graphs;
alpha zero/ordinary/near-one; personalization equal to pi; bipartite graphs;
clique formula with nonuniform personalization; wrong IDs/truncation/NaN;
modified output values; hostile ambient Decimal settings; refusal cleanup;
reservation before scans; nonclique refusal; original source cursor lifetimes.

Use new files only. Keep the eleven measured native modules and all prior
study receipts unchanged. An independent bounded theorem/prior-art review
will challenge the formulas while lead work tests the new implementation.

## Completed Review And Implementation

The [independent bounded review](PageRank-Native-Direct-Review.md) is terminal
and lead-replayed: 43 graphs, 1,180 exact PageRank cases, 7,080 actual-output
bounds, 72 clique cases, four native-degree fixtures and two zero-bias
counterexamples. It accepts both formulas under the stated conditions, not
the implementation or a novelty claim. Grolmusz's
[full text](https://arxiv.org/html/1205.1960) already gives the stationary-
difference resolvent identity and stationary-personalization equality.

Its Decimal square-root warning is met by the reused verified helper:
`upper.next_plus(upper.sqrt(value))` for positive finite values, exact zero
for zero. Setting a ceiling context alone would be unsafe. The checker
demonstrates that pitfall with the three-digit result sqrt(2)=1.41.

`experiments/boolean_rank_native_direct.py` implements both controls with
separate actual-byte certification. Clique coefficients are exact rational
scalars prepared once; stationary targets are classwise degree/volume
fractions. The stationary verifier sums squares of individual personalization
weights, not the square of their class sum. No P-record scratch is written.
The candidate file remains private through verification and is linked without
replacement only on admission. Mutation metadata is detection under stable
ownership, not hostile concurrent-write protection.

Eight direct-control tests pass, including 108 small stationary/perturbed
exact-answer comparisons, clique cases, invalid output, ambient Decimal
settings, SQLite cancellation cleanup, and consumed-prefix write detection.
Two measurement-contract tests pass. The initial six control tests and two
measurement tests were observed failing for missing modules before code was
added. This is lead testing, not an independent implementation audit.

## Predeclared Direct Comparison

Use the same ten frozen prepared sources as the native study. Do not rebuild
or select more favorable inputs. Run stationary on all ten cases; run clique
direct only on the two star cases, whose original graph is complete. Choose
factor/generic as the ordinary-damping control and factor/native as the
near-one control, following the previous measured portfolio.

For each of twelve candidate/case combinations, run three fresh serial
control/candidate/control brackets: 108 attempts and 36 brackets. Retain
refusals and all timing observations. The predeclared stability screen is
before/after control max/min <=1.5. Audit both full paired outputs whenever
all three admit. Do not compute speed-to-answer against a refused control.
Process wall time and query time are separate; source-open through source-close
query timing includes preflight, full output, certification and publication.
Prepared-source build cost is reused and explicitly linked, not claimed zero.

Freeze all eleven native implementation hashes plus these two new modules.
No review probes or other experiments run during timing. Worker RSS is
observed, not capped; no cold-cache or tail-percentile conclusion is planned.
The strongest claim available is a comparison among these implemented plans
on these synthetic sources, not general Neo4j superiority or novel mathematics.

## Completed Results

The driver exited zero after **108 attempts: 87 admitted outputs and 21
refusals**. The 36 brackets include 18 with all three outputs admitted; all
18 pass the stability screen and both paired-output audits. Every before/after
control pair, including refused cases, met that timing screen. Complete
admitted output contains 247,236 original-ID scores and 3,955,776 bytes.
All ten input databases and the prior native receipt are reused unchanged.

| Case | Candidate | Candidate median ms | CG median ms | Stable paired query reduction | Admission |
|---|---|---:|---:|---:|---|
| Cycle, near one | Stationary | 1.97 | 5.07 | 61.07% | Both admit |
| Path, near one | Stationary | 4.06 | 30.58 | 86.72% | Both admit |
| Chorded, near one | Stationary | 9.56 | 45.15 spent refusing | Not defined | Stationary admits; CG refuses |
| Star, near one | Stationary | 3.99 | 8.99 | 55.62% | Both admit |
| Dense incomplete, near one | Stationary | 115.80 | 342.99 | 66.93% | Both admit |
| Star, 0.85 | Exact clique | 6.17 | 7.80 | 21.13% | Both admit |
| Star, near one | Exact clique | 5.99 | 8.99 | 33.36% | Both admit |

Reductions are medians of the three local candidate/control ratios, not ratios
of unrelated table medians. Each candidate has three observations. The star
CG control has twelve observations per damping because it brackets two
candidates; other controls have six. There are no tail-percentile claims.

At ordinary alpha=0.85, **stationary refuses on all five sources**, with error
upper bounds approximately 0.12-0.57 versus required 1e-10. Refusal times are
1.83 ms cycle, 3.91 ms path, 9.38 ms chorded, 3.96 ms star and 114.70 ms dense.
These are not speedups to an answer. The twenty-one refusals comprise fifteen
ordinary stationary attempts and six near-one chorded CG controls. No failure
was discarded or retried on a more favorable input.

Near-one stationary bounds are approximately 1.47e-14 (cycle), 2.42e-13
(path), 2.35e-13 (chorded), 1.48e-16 (star), and 4.71e-13 (dense). Their
actual-output distance terms are included, not just the ideal stationary
bias. On the clique, exact-target bounds are about 4.35e-17 at ordinary
damping and 5.46e-17 near one. These are upper bounds, not measured exact
PageRank errors on the large fixtures.

### Resource Outcome

Both direct controls use no iterative rank vector, one full output read and
zero P-class certificate scratch after paid O(F) topology validation. The
largest dense comparison avoids the previous 486,186-byte scratch file and
reads 221,184 rather than 442,368 output bytes in certification. This is a
50% reduction of **certificate output reads**, not all end-to-end I/O.
The complete 221,184-byte answer still has to be staged, verified and delivered.

Observed worker peak RSS across these runs spans **20.91-22.03 MiB**. Mode
medians overlap around 20.91-21.03 MiB; there is no established whole-process
RAM win. Input/cache/runtime/buffers and validation remain present. The direct
clique route conservatively pays the native gap validator even though its
closed-form target does not need that gap; this is a reuse cost, not a minimal
clique-validation theorem. These are prototype Python measurements, not Rust,
physical-cap enforcement or a production graph benchmark.

Preparation costs remain those of the frozen native study, roughly 4-86 ms
on these small inputs, plus separately recorded inspection. A fresh source
does not become free merely because a reused query avoids iteration. Changed
source refresh, cold-device behavior and total peak temporary disk are still
unmeasured.

## What The Stronger Control Falsifies

1. The near-one advantage in the earlier study does **not** show that the
   reduced iterative solver is necessary. A standard stationary candidate
   is cheaper, less stateful and more readily certifiable on this sample.
2. The previous chorded refusal is not a fundamental inability to satisfy
   the query. Changing the candidate, while keeping the same original-answer
   contract and proved gap, produces an admitted answer.
3. The star's earlier iterative win was an incomplete comparator. Its clique
   closed form wins at ordinary damping; the stationary approximation is
   faster near one at the stated absolute tolerance.
4. None of these observations makes degree centrality a replacement for
   personalized PageRank at normal damping. All normal stationary candidates
   refused, correctly preserving that distinction.
5. Nothing here establishes novel algebra or a physical low-RAM product.
   The useful contribution of this follow-through is a stronger, falsifiable
   portfolio and an honest account of when solving can be skipped.

### Stop Rule For This A01 Branch

Do not create another precision, gap or CG variant merely to improve these
engineered fixtures. The next substantive A01 gate is a useful real-source
overlap query and comparison against the direct controls now implemented.
A further research claim needs a distinct theorem, wider useful eligibility,
or a complete resource-constrained separation from published methods. The
seven-family goal remains active; turn to the remaining families' unresolved
source, theorem and full-workflow issues rather than treating this control
as a new algorithmic discovery.

## Reproducibility

Evidence is in `evidence/boolean-rank-direct-20260921/`:

```text
receipt.json SHA256
78ffb2a1d70d17ea604a90cf13fc6fd17afd9978601a8c23016764a912716024
events.jsonl SHA256
45e23d541d08cba3d1f0ee81175b0da1c12e62fa3c92d8cbce81e976ea782785
```

The combined Boolean suite passed **147 tests with warnings treated as
errors** before measurements; none of its measured modules changed afterward.
This includes the direct tests and earlier solver, source, byte-certificate,
failure-lifecycle and measurement tests. It is not a full repository test run
or an independent implementation audit.

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B measure_boolean_rank_direct.py \
  --source-directory ../evidence/boolean-rank-native-20260921 \
  --directory ../evidence/boolean-rank-direct-new-run
```

This retained-evidence checker runs from the repository root. It validates
hashes, complete generated IDs, event receipts, all refusals and paired
output consistency, not new PageRank ground truth or implementation security.

```python
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import math
import struct
import sys

root=Path('research_algorithms_20260920')
sys.path.insert(0,str(root/'experiments'))
from measure_boolean_rank_workflow import audit_boolean_output_pair
evidence=root/'evidence/boolean-rank-direct-20260921'
inputs=root/'evidence/boolean-rank-native-20260921'
def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()
assert digest(evidence/'receipt.json')=='78ffb2a1d70d17ea604a90cf13fc6fd17afd9978601a8c23016764a912716024'
assert digest(evidence/'events.jsonl')=='45e23d541d08cba3d1f0ee81175b0da1c12e62fa3c92d8cbce81e976ea782785'
data=json.loads((evidence/'receipt.json').read_text())
assert digest(inputs/'receipt.json')==data['prior_receipt_sha256']
assert [json.loads(x) for x in (evidence/'events.jsonl').read_text().splitlines()]==data['queries']
for name,expected in data['source_hashes'].items():
    assert digest(root/'experiments'/name)==expected,name
for item in data['reused_preparations']:
    assert digest(inputs/item['database'])==item['database_sha256']
queries={q['trial']:q for q in data['queries']}
assert len(queries)==108
admitted=rows=output_bytes=audits=brackets=0
for q in queries.values():
    path=evidence/q['output']
    assert q['source_events']['active_cursors']==0
    certificate=q['pipeline']['certificate']
    assert q['accepted']==certificate['accepted']
    if not q['accepted']:
        assert not path.exists()
        assert q['failure_type'] in ('NativeDirectCertificateRefusal','NativeRankCertificateRefusal')
        continue
    assert digest(path)==certificate['output_sha256']==q['pipeline']['output_sha256']==q['postprocess_output_sha256']
    assert Fraction(certificate['l1_error_upper'])<=Fraction(q['pipeline']['epsilon'])
    count=0
    with path.open('rb') as stream:
        while record:=stream.read(16):
            identifier,score=struct.unpack('<Qd',record)
            assert identifier==count and math.isfinite(score) and score>=0
            count+=1
    assert count==certificate['nrows']==q['pipeline']['output_rows']
    assert path.stat().st_size==16*count==q['pipeline']['output_bytes']
    if q['mode'] in ('stationary','clique-direct'):
        assert certificate['row_passes']==1 and certificate['scratch_bytes']==0
        assert certificate['output_read_bytes']==16*count
    admitted+=1
    rows+=count
    output_bytes+=16*count
for b in data['brackets']:
    before,candidate,after=[queries[t] for t in b['trials']]
    ratio=max(before['query_seconds'],after['query_seconds'])/min(before['query_seconds'],after['query_seconds'])
    assert b['control_time_ratio']==ratio
    assert b['timing_stable']==(ratio<=1.5)
    assert b['all_accepted']==all(q['accepted'] for q in (before,candidate,after))
    if not b['all_accepted']:
        continue
    assert b['timing_stable']
    brackets+=1
    for label,control in (('before',before),('after',after)):
        bound=sum(Fraction(q['pipeline']['certificate']['l1_error_upper']) for q in (candidate,control))
        assert audit_boolean_output_pair(evidence/candidate['output'],evidence/control['output'],bound)==b['audit_'+label]
        audits+=1
assert (admitted,rows,output_bytes,brackets,audits)==(87,247236,3955776,18,36)
print('Direct evidence verified: 87 outputs; 247236 rows; 36 exact pair audits; 21 refusals.')
print('Ten prepared-source hashes, thirteen implementation hashes and prior receipt match.')
```

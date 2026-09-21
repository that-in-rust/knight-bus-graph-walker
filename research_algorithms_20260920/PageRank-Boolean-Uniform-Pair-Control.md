# Boolean PageRank: Uniform All-Pair Direct Control

## Scope and Status

This is a same-source direct numerical control for the prepared native Boolean
membership source. It is an ordinary symmetry/inverse specialization, not a new
generic PageRank algorithm. For this exact family, an iterative factor/class CG
baseline alone is incomplete: the stronger structural baseline evaluates the
closed form below without iterations or a dense matrix. No timing or general
performance superiority is claimed.

The class graph is the triangular graph T(F) = J(F,2), with each class replaced
by an h-clique. The triangular/Johnson identification is standard; see
[Brouwer's Johnson graph reference](https://aeb.win.tue.nl/graphs/Johnson.html).
The blow-up spectrum and inverse used here are derived below rather than assumed
from that reference.

Owned implementation: `experiments/boolean_rank_uniform_pair_control.py`.
Owned tests: `experiments/test_boolean_rank_uniform_pair_control.py`.
The same owned module now also supplies an exact analytic-target certificate.
Shared validator, prepared source, existing local lift, general/deflated
verifiers, and integration remain separately owned. Both owned APIs import the
real shared validator; there is no production or test stub for it.

## Executable Contract

- REQ-UP-001: WHEN a source supplies F >= 4, every distinct canonical pair
  (a,b), 0 <= a < b < F, common height h >= 1, and no other classes, THEN the
  solver SHALL accept its validated topology, with P = F(F-1)/2 classes,
  n = hP vertices and d = h(2F-3)-1. The shared validator also requires the
  exact class-weight total, source counts/snapshot, and d <= 2^53 for the lift.
- REQ-UP-002: WHEN any topology premise fails, including at alpha = 0, THEN
  the solver SHALL refuse; it SHALL NOT silently fall back to another graph.
- REQ-UP-003: WHEN full supplied F exceeds `max_factor_slots`, THEN admission
  SHALL fail before source scans or factor allocation, even if fewer factors
  have nonzero personalization or memberships. The cap is a count, not bytes.
- REQ-UP-004: WHEN alpha is a finite binary64 in [0,1), THEN the solver SHALL
  use its exact Fraction value and frozen p_v = Fraction.from_float(w_v)/W.
  Every nonnegative finite input weight is admitted, provided W > 0. No uniform
  personalization assumption, float total, or early normalized-weight rounding
  is allowed.
- REQ-UP-005: WHEN preparing exact factors, THEN F incident dyadic sums SHALL
  be replaced in place by F exact rational S values. Only the numerical solver
  SHALL additionally allocate F returned doubles. Neither API SHALL retain a
  P/n vector, pair list, dense matrix, or iterative operator state.
- REQ-UP-006: WHEN a class scan fails or a consumer raises, THEN all cursors
  opened by that scan SHALL close without draining. The source stays caller-owned.
- REQ-UP-007: WHEN certifying a valid `<Qd` output, THEN the certificate SHALL
  compare the exact binary64 score values with exact analytic PageRank targets,
  report their exact L1 distance, and accept exactly when it is <= epsilon.
- REQ-UP-008: WHEN certifying, THEN one full forward output-byte scan SHALL
  validate source class/ID order, count, finite nonnegative scores, SHA-256,
  and pinned snapshot. There SHALL be no output rewind, P scratch, or n/P array.
- REQ-UP-009: WHEN output/source data is malformed, reading fails, or execution
  is interrupted, THEN the output descriptor and active source cursors SHALL
  close. Well-formed but inaccurate output SHALL still receive a full error/hash
  receipt, with `accepted=False` rather than early rejection.

The public API is:

```python
solve_uniform_pair_rank_state(source, *, alpha, max_factor_slots)
```

The state has exactly `snapshot_id`, `alpha`, `gamma`, `method`, `factor_scores`,
`class_scores`, and `metrics`. Here `gamma = Fraction(1)-Fraction.from_float(alpha)`,
`method = 'uniform-pair-direct'`, `factor_scores` is an `array('d')` of length F,
and `class_scores = None`. The state feeds the unchanged
`iterate_boolean_rank_output(source, state)`. It is provisional, not certified.

## Spectral Decomposition

Let C be the F-by-P incidence matrix whose column for pair {a,b} contains two
ones. It is notation only: neither C nor any following matrix is allocated by
the implementation. Counting incident pairs gives

```text
C C^T = (F-2) I_F + J_F.
```

Thus C has rank F for F >= 4. The class adjacency matrix is C^T C - 2I_P.
Within a class, every two distinct vertices are adjacent; across two distinct
classes, adjacency means sharing a factor. Therefore the vertex adjacency is

```text
A = (C^T C - I_P) tensor J_h - I_n.
```

This is the Boolean union, with no loops and no double-counted shared edges.
The constant-class vectors split into the constant mode, the lifted factor
contrasts, and ker(C). Within-class zero-sum vectors form the fourth space.
Their adjacency eigenvalues and dimensions are:

| Space | Adjacency eigenvalue | Dimension |
| --- | --- | --- |
| Constant mass | d = h(2F-3)-1 | 1 |
| Factor contrasts | e2 = h(F-3)-1 | F-1 |
| Pair contrasts in ker(C) | -h-1 | P-F |
| Within-class contrasts | -1 | P(h-1) |

The dimensions sum to n. At h=1 the fourth space is absent; at F=4,h=1,
e2=0. The transition operator T=A/d is symmetric and doubly stochastic. For
orthogonal projectors E0,E2,E3,E4 and gamma=1-alpha, its exact PageRank inverse is

```text
pi = E0 p
   + gamma/(1-alpha*e2/d) E2 p
   + gamma/(1+alpha*(h+1)/d) E3 p
   + gamma/(1+alpha/d) E4 p.
```

Here E0 p = 1_n/n. The cancellation of gamma on the mass mode is analytic;
the implementation never numerically divides that mode by a tiny 1-alpha.
All other denominators stay positive, including at the greatest float below 1.
The shared `spectral_upper=e2/d` is the largest nonconstant eigenvalue, NOT
the largest absolute nonconstant eigenvalue. For example, at F=4,h=1 the latter
is 1/2, although `spectral_upper` is zero.

## Direct Factor Formula

Write W_ab for the exact prepared class weight, W=sum_ab W_ab, and
W_a=sum_{b != a} W_ab. Let q=pi/d and S_a=sum_{v incident a} q_v. The factor
aggregate annihilates E3 and E4. Its mass-mode component is 2/(F*d), and its
factor-contrast component is W_a/W - 2/F. Applying the inverse on those two
remaining spaces yields the requested exact expression:

```text
S_a = 2/(F*d) + (1-alpha)*(W_a/W - 2/F)/(d-alpha*e2).
```

As a separate algebra check, let T_ab=sum_{v in class ab} q_v and
p_ab=W_ab/W. The original stationary equation, before any spectral argument, is

```text
(d+alpha) q_v = gamma*p_v + alpha*(S_a+S_b-T_ab)
[d+alpha*(1+h)] T_ab = gamma*p_ab + alpha*h*(S_a+S_b).
```

Summing the second identity over the F-1 classes incident to a gives

```text
[d-alpha*(h*(F-3)-1)] S_a = gamma*(W_a/W) + alpha*h*sum_b S_b.
sum_b S_b = 2/d.
```

Finally d-e2=hF makes this expression identical to the boxed factor formula.
At alpha=0 it reduces exactly to S_a=W_a/(W*d), including S_a=0 for a factor
with no preference mass. Nonuniform weights within a class remain meaningful:
they live in E4 and are restored by the vertex lift, not lost by aggregation.

The unchanged lift evaluates the two local identities with the rounded S values.
It streams the exact per-vertex preference and original IDs from the native
source. Its floating arithmetic adds rounding beyond the once-rounded factor
scores. This control does not claim exact final binary64 PageRank or an actual
output certificate merely because its pre-rounding algebra is exact.

## Paid Work and Storage

No end-to-end comparison may hide preparation, validation, output, or certificate
work. The logical counts below are not physical-memory limits or timings.

| Phase | Work that must be paid | Retained or external state |
| --- | --- | --- |
| Native preparation | Read n original IDs/weights and 2n memberships; insert F factor records; create the signature index; rescan n vertices; create P exact class totals/degrees | SQLite stores n vertices, F factors, P classes plus indexes; exact totals and current class state |
| Eligibility validation | One P-class scan, exact weight-total check, canonical pair/count/height/degree/snapshot checks | Constant row metadata and exact scalar total; no list of pairs |
| Direct accumulation | One additional P-class scan and exactly 2P exact additions into incident totals | F dyadic Fraction sums, plus constant scalar metadata/temporaries |
| Direct evaluation | O(F) exact rational evaluation and mass-consistency checking; solver only: F final conversions | F dyadic sums become F exact S in place; solver adds F packed doubles |
| Full local output | P classes and n original vertex records; exact preferences; n local floating lifts | F scores, current class/vertex, bounded cursors; publication is 16n bytes for `<Qd` records |
| Exact analytic-target certificate | Fresh validation/accumulation: 2P class records; target comparison: P classes and n vertices; one 16n-byte output scan and SHA-256 | F exact S, constant class/vertex/error state; zero scratch, no packed score array |
| Existing actual-output residual certificate | Two P-class/n-vertex passes; read output twice, hash both passes, enclose original residual with directed rounding | Two F-sized Decimal endpoint arrays, sequential P-record private scratch, scalar state |

Preparation uses indexed SQLite operations and sorting/index construction; the
row counts do not assert O(n) total CPU/I/O. Its B-tree costs, journal/commit
work, database bytes, and exact arithmetic must be charged. Native input avoids
expanding nd/2 Boolean edges, but it does not avoid reading n input weights.

The solver reports `validation_class_rows=P` and `validation_class_passes=1`
separately from `class_records=P` and `class_passes=1` for its accumulation.
Hence a fresh solve reads 2P decoded class records, not P. There is no amortized
validation reuse hidden in these counts. `matrix_applications=iterations=0`.

The direct arithmetic is O(P+F) operations on variable-size integers, NOT
O(P+F) fixed-cost binary64 operations. An exact sum of k finite nonnegative
binary64 weights has a reduced power-of-two denominator with at most 1075 bits
and a numerator with at most 2098+ceil(log2(k)) bits (zero has zero numerator
bits). Here each factor sum contains k=h(F-1) vertex weights. The scalar W and
Fraction intermediates also have real bit costs; multiplication, division, and
gcd costs are not modeled as unit-time hardware operations.

`exact_factor_sum_slots=F` explicitly charges the exact sum vector.
`exact_factor_numerator_bits_peak` and `exact_factor_denominator_bits_peak`
track the maximum aggregate reduced-integer bit payload after each slot update;
the two maxima need not occur simultaneously. Corresponding `_final` fields
describe the retained sums at the end of accumulation. Metrics are maintained
incrementally, without rescanning F slots after every update. A mixed largest
finite/subnormal fixture charges over 2000 numerator bits per slot, not 64.

The shared preparation helper then transforms that same list in place into exact
rational S values. `exact_factor_fraction_slots=F` charges these slots, while
`exact_factor_state_numerator_bits_peak` and the corresponding denominator field
cover the whole preparation, including the mixed old-sum/new-S conversion state.
Their `_final` fields describe the exact S vector, whose denominators need not
be powers of two. The solver rounds that vector; the certificate never does.

`factor_score_payload_bytes=F*array('d').itemsize` describes ONLY the packed
returned scores. It is not a total-memory bound. Fraction/Python integer object
headers, list pointers, allocator rounding, simultaneously live old/new scalar
operands, arithmetic temporaries, cursor/SQLite caches, input artifact, output,
and verifier state are additional. There is deliberately no fake
`vector_payload_upper_bytes=8*F` model for the exact solver.

For arbitrary same-source class weights, reading all P class records is an
Omega(P) information requirement; writing F factor values is Omega(F). Requiring
the complete original-ID output adds Omega(n) output work and storage regardless
of how fast this inverse is evaluated. These lower bounds do not prove a fastest
implementation or a wall-clock win; they identify the ordinary strong baseline
that any proposed acceleration on this family should include.

## Exact Target Certificate

For this family, a general or mass-deflated residual bound is not the strongest
known same-source correctness control: the exact target is explicitly available.
Comparing directly to it gives an exact L1 error with no residual-to-error slack.
This is ordinary analytic-target comparison, not a new theorem or generic
algorithm. Any claimed gain for deflation must account for this simpler
structural control and its one-pass/no-scratch resource profile. That does not
establish lower wall-clock cost for arbitrary-precision Fraction operations.

```python
certify_uniform_pair_exact_output(
    source, output_path, *, alpha, epsilon, max_factor_slots
)
```

The helper used by both public APIs prepares exact S, but the certificate neither
calls the rounded-state solver nor converts S to float. With a class ab, define
B_ab=gamma*W_ab/W. The exact class q-total and exact vertex target are

```text
Q_ab = (B_ab + alpha*h*(S_a+S_b)) / (d + alpha*(1+h))
x_v  = d * (gamma*w_v/W + alpha*(S_a+S_b-Q_ab)) / (d+alpha).
```

These are the independently derived local stationary identities above, with
every weight interpreted as `Fraction.from_float(w_v)`. The implementation
precomputes the global coefficient `d*gamma/((d+alpha)*W)` and one class offset
`d*alpha*(S_a+S_b-Q_ab)/(d+alpha)`. It multiplies each exact vertex weight by the
global coefficient, adds the offset, and accumulates

```text
L1_exact += abs(Fraction.from_float(published_score_v) - x_v).
```

There is no `1/(1-alpha)` certificate amplification. This remains an exact
comparison at `nextafter(1.0,0.0)` and preserves unequal preferences within a
class. It can certify any well-formed candidate output from this source, not
only the owned solver's output.

The required report includes `accepted`, `l1_error_upper` (an exact rational
string, in fact equal to the L1 error), `output_sha256`, `nrows`, `row_passes=1`,
`scratch_bytes=0`, `snapshot_id`, and `scope`. `epsilon` admits a positive finite
float, integer, or Fraction, interpreted exactly; equality is accepted. Alpha
retains the solver's strict finite-float [0,1) contract. Malformed data raises
`ValueError`; source/I/O failures and interruption propagate after cleanup.

The file is opened once, unbuffered, after parameter/topology admission. An
initial size check requires exactly 16n bytes. Each record's uint64 ID must equal
the current original source ID; source IDs must increase within each class.
Scores and source weights must be finite/nonnegative. Per-class source row
counts and exact weight sums are checked. A final EOF check, descriptor
size/mtime/ctime comparison, and source snapshot check reject observed changes.
The digest identifies the exact bytes consumed, not a future pathname state.
A single scan plus stat checks does not claim an atomic snapshot against an
adversarial writer capable of concealing concurrent changes.

### Resource Accounting

The fresh certificate retains F Fraction slots, initially dyadic incident sums
and then exact rational S in place. There is no F-double result vector and no
P/n array. It uses current-class Q/offset/weight scalars, the current vertex,
one exact L1 accumulator, constant digest state, and nested closeable cursors.
`retained_fraction_values=F` counts vector slots only, not these scalar values.
`factor_score_payload_bytes=0` is NOT a claim that certification uses zero memory.

The report's `class_passes=3` and `class_records=3P` explicitly pay one shared
validation pass, one exact incident-weight pass, and one class/vertex comparison
pass. `row_passes=1` refers to actual output/vertex rows. `output_read_bytes=16n`
charges the single actual-byte scan; the final EOF probe returns zero bytes.
There is no scratch creation or output rewrite. No solve-time S cache is reused
or silently amortized by the certificate API.

In addition to the F-vector integer bit accounting above,
`l1_error_numerator_bits_peak` / `l1_error_denominator_bits_peak` measure the
running exact error scalar and `exact_target_*_bits_peak` measure the per-vertex
target scalar. These are bit payloads, not physical memory bounds. Class offset,
Q, scalar coefficients, exact class-weight checks, tolerance, old/new Fraction
operands, multiplication/division/gcd temporaries, Python object/allocator
overhead, and SQLite cache remain additional costs. Very wide input dyadics or
published scores can make exact arithmetic costly even though no n/P state is
retained. The API caps full F, not numerator growth or RSS.

## Residual Comparators

For actual published x and r=gamma*p+alpha*T*x-x, the generic contraction
certificate uses ||pi-x||_1 <= ||r||_1/gamma. Its two passes and directed
rounding still have to be paid; near-one alpha can make the bound too weak even
for an accurate output. Analytic correctness is not a substitute for verifying
the actual `<Qd` bytes and snapshot.

The known symmetric mass-separated inverse also gives a stronger baseline here.
Let m=sum(x), u=1_n/n, r_perp=r-gamma*(1-m)*u. Since T is symmetric,

```text
||pi-x||_1 <= |1-m| + sqrt(n)*||r_perp||_2/(1-alpha*e2/d).
```

Negative eigenvalues do not weaken this denominator: every 1-alpha*lambda is
positive and minimized at the largest nonconstant eigenvalue. A certified use
must independently enclose mass, residual, projections, norms, and this bound
on actual output bits with directed arithmetic. Such a verifier still pays for
original-source/output scans, F factor totals, and per-class totals or scratch;
it is not a free consequence of having computed S. The main lead owns these
general/deflated residual verifiers and pipeline integration. The owned exact
target certificate above is a separate strong control, not an implementation
or endorsement of a new deflation theorem.

## Verification Receipts

Tests were written before the production module. Initial red discovery ran
10 tests with 53 failing subcases, each reporting that the direct solver was
absent. A focused red run of `test_exact_accumulator_accounting` confirmed that
single failure independently. After implementation, all initial 10 tests passed.

The test-owned oracle expands the original loop-free Boolean adjacency and uses
Fraction Gaussian elimination. Tests compare its exact pi with a separately constructed
four-space spectral projection and verify that the proposed exact S equals the
oracle's incident pi/d sums before comparing any rounded output. Cases include
F=4,5,6, h=1,2,3, zero weights, one-vertex preference, arbitrary seeded dyadics,
subnormals, largest finite weights whose totals overflow float, tiny alpha, zero
alpha, and `nextafter(1.0, 0.0)`.

Further coverage checks the public state shape, full-F admission before scans,
invalid topology even at alpha=0, no solver vertex scan, exact accumulator bit
charges, native one-shot/unsorted/duplicate-membership input normalization,
snapshot binding, cursor decode failure in both passes, consumer-side dyadic
rejection with an open cursor, and a snapshot mutation during the solve.

Run the owned tests from the repository root:

```sh
/opt/homebrew/bin/python3 -m unittest discover -s research_algorithms_20260920/experiments -p test_boolean_rank_uniform_pair_control.py -v
```

The broader source/validator/lift run passed all 39 tests before the final two
cleanup/snapshot tests were added, on Homebrew Python 3.14.4 / SQLite 3.53.0.
The default `/usr/bin/python3` (Python 3.9.6 / SQLite 3.43.2) passed the owned
tests but the broader run had one pre-existing source-test error:
`test_readonly_pinned_snapshot` raised `sqlite3.OperationalError: unable to open
database file` while configuring the special-character WAL fixture. It also
failed in isolation, without importing this solver. No shared source/test file
was changed to address that runtime-dependent result.

Final correctness receipts, with warnings treated as errors:

```text
/usr/bin/python3 -W error -m unittest discover -s research_algorithms_20260920/experiments -p test_boolean_rank_uniform_pair_control.py -v
Ran 12 tests: OK

# From research_algorithms_20260920/experiments:
/opt/homebrew/bin/python3 -W error -m unittest test_boolean_rank_uniform_pair_control test_boolean_rank_spectral_source test_boolean_rank_sqlite_source test_stream_boolean_rank_solver -q
Ran 41 tests: OK
```

Those are the initial solver-only terminal receipts. The follow-up exact-target
certificate resumed correctness work in the same three files. Its initial nine
TDD tests all failed with "uniform-pair exact-target certificate is absent".
After the implementation/refactor all 21 owned tests passed; strengthened
coverage then passed all 24 owned tests. Certificate checks include exact
equality with the test-owned dense oracle, arbitrary/unequal/extreme weights,
near-one alpha, exact tolerance boundaries, actual solver output, malformed
bytes/IDs/scores, source counts/weights, one open/no rewind/no scratch, descriptor
mutation, snapshot mutation, I/O failure, and native-source interruption cleanup.

No timing benchmark, commit, README edit, old benchmark implementation edit, or
unrelated document edit was performed. General/deflated verifier integration
remains with the main lead.

Updated terminal receipts, warnings treated as errors:

```text
/usr/bin/python3 -W error -m unittest discover -s research_algorithms_20260920/experiments -p test_boolean_rank_uniform_pair_control.py -q
Ran 24 tests: OK

# From research_algorithms_20260920/experiments:
/opt/homebrew/bin/python3 -W error -m unittest test_boolean_rank_uniform_pair_control test_boolean_rank_spectral_source test_boolean_rank_sqlite_source test_stream_boolean_rank_solver -q
Ran 53 tests: OK
```

The exact-target certificate follow-up is correctness-terminal in the three
owned files. No timing measurements were performed; no other agent's terminal
status is inferred from these owned receipts.

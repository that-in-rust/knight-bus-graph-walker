# Independent Streaming Certificate Review

Date: 2026-09-21. Bounded mathematical and closest-art sidecar review, not an implementation audit or publication-readiness approval. Only this document was written. No implementation, new tests, or benchmark drivers were read or run; no public timing was performed. Small arithmetic checks ran in memory with Python 3.9.6, `fractions.Fraction`, and explicit binary64 byte decoding.

Reviewed proposal: [PageRank-Streaming-Publication-Certificate.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/PageRank-Streaming-Publication-Certificate.md), SHA-256 `36712c71c872f1ee58bf2c10d45d050f0b86c48b9178009327eeed805333c476`. Supporting context: the native-incidence evidence and previous mass-correction review. Conclusions apply to that proposal snapshot, not concurrent implementation work.

## Findings First

1. **Qualified mathematical support:** equations (1)-(4) are valid under the declared exact source, outward-enclosure, sequential binary64, and same-output-value contracts. Neither signed `h` nor lack of exact mass normalization invalidates the argument. I found no counterexample within those contracts.
2. **Closest-art blocker:** Higham alone is insufficient positioning. Rump's 2012 computable summation bound supplies a strictly stronger same-state control. The proposed gamma admission boundary is a limitation of this proof choice, not of low-memory certification generally.
3. **Trust-boundary blocker for an unconditional guarantee:** scalar ledgers do not validate degrees/cardinalities, the builder, or serialization. A two-vertex corrupted-degree example below has zero factor residual but true L1 error `1/2`. Hashing the corrupted input does not repair the proof.
4. **Byte-boundary qualification:** hashing the write buffer establishes byte identity, not correspondence between those bytes and the values used in `eta_i`. That correspondence, output order/length, and a consistent score/certificate transaction remain separate obligations. The proposal acknowledges this shared-failure risk; this review does not establish that any implementation satisfies it.

The defensible research claim is a potentially useful integration of established validation techniques with native-incidence traversal and transactional output. A new summation theorem, new residual principle, or demonstrated optimal RAM/accuracy frontier is not supported.

## Mathematical Audit

### Isolates And Original Residual

Let `Z` denote isolated vertices and `c=(1-a)/(1-a*pZ)`. Symmetry and nonnegative incidence weights imply that zero degree means no incoming or outgoing projected edges. If `t=sum_Z x*_i`, the original dangling-to-`p` rule gives

```text
t = ((1-a)+a*t)*pZ,
t = (1-a)*pZ/(1-a*pZ),
x*_Z = c*p_Z.
```

For the ideal candidate with `X_Z=c*p_Z`, its dangling mass equals this exact value even when active `X` is signed or has the wrong total mass. Thus the original full-vector residual `R=(1-a)p+a*P*X-X` satisfies

```text
R_active = c*p_active + a*(B B^T-Q)*y - D*y
         = a*B*(B^T*y-h),
R_Z      = 0.
```

Here `P` is the fixed full column-stochastic transition matrix, including dangling redistribution. Consequently `||(I-aP)^-1||_1=1/(1-a)` and `||B*r||_1 <= sum_f s_f*|r_f|`. This establishes (1) without positivity of `h`, `r`, or ideal `y`. The usual residual-to-error principle is established PageRank mathematics; see Gleich, *PageRank Beyond the Web*, Section 2, Aside 3, in the [primary preprint](https://arxiv.org/html/1407.5107).

With `F=0` and `N>0`, `pZ=1`, `c=1`, and `x*=p`; only publication error remains. `N=0` needs an explicit refusal or separate empty-answer contract because a normalized personalization does not exist. General directed dangling vertices can have incoming edges, so the isolate elimination must not be exported to that setting. For uniform personalization, use exact `|Z|/N`, not a sum of rounded `1/N` values.

### Row Weights And Signed States

Writing `delta_i=yhat_i-y_i`, the complete weighted row-error calculation is

```text
s^T |B^T delta| <= s^T B^T |delta|
                = (B*s)^T |delta|
                = (d+q)^T |delta| <= Erow.
```

Repeated groups are separate columns and contribute separately. Duplicate membership inside one factor is outside the binary contract. Neither `d` alone, `q` alone, nor an unweighted error sum is a valid general replacement for `d+q`.

This is a specialization of the elementary nonnegative-matrix inequality `w^T|M*v| <= (M^T*w)^T|v|`. The useful integration detail is that the resulting weight is already available in each row, not a new inequality.

Signed finite dyadic `h` is allowed. Cancellation in `sum h_f` must be enclosed, but the positive-sum proof only requires nonnegative **scatter operands** `yhat`, not nonnegative `h` or exact `y`. A negative exact `y` can even round naturally to negative zero; its distance from zero still belongs in `e_i`. This is not permission to silently clip negatives or erase conversion error. Ordinary negative rounded rows require refusal or a different summation proof.

### IEEE Admission

The positive-normal rule is sufficient for the stated scatter analysis: starting at zero, every nonzero exact sum of admitted nonnegative operands is at least the minimum normal; every finite rounded partial sum remains nonnegative and normal. There is no cancellation-driven underflow in these additions. Zero operands and either signed zero have real value zero.

The contract must nevertheless fix round-to-nearest/ties-to-even binary64 additions with a rounding point at every update. Reject overflow, NaN, negative operands and forbidden subnormals **before** using the bound. A format name alone does not establish the rounding mode, compiler reassociation policy, precision of intermediates, or absence of lost updates. FTZ/DAZ does not affect this particular all-normal positive scatter, but can affect row-value generation; either disable it throughout or prove those conversions separately.

Higham's [1993 primary paper](https://nhigham.com/wp-content/uploads/2023/10/high93s.pdf), Sections 1-2, supplies the standard relative-error model and recursive-sum estimate. Using `m=s` instead of `s-1` harmlessly counts the exact first addition. The inverse conversion in (3) is valid when `s*u<1/2`; for binary64 this is the exact integer admission `s<2^52`. Equivalently the proposed coefficient is `s*u/(1-2*s*u)`. Its denominator needs a positive lower enclosure and the coefficient an upper enclosure.

Positive-normal admission is conservative, not an inherent IEEE restriction on addition. Gradual-underflow extensions require an explicit justification, not silent relaxation of this proposal's chosen policy.

### Emitted Output And Equation (4)

The endpoints imply both `|yhat_i-y_i|<=e_i` and `|xhat_i-X_i|<=eta_i`, including sign-crossing intervals. Therefore

```text
sum_f s_f*|T_f-h_f|
  <= sum_f s_f*|z_f-h_f| + sum_f s_f*|z_f-Q_f|
     + sum_f s_f*|Q_f-T_f|
  <= Rhat + Escatter + Erow,

||x*-xhat||_1 <= ||x*-X||_1 + ||X-xhat||_1
               <= a/(1-a)*(Rhat+Escatter+Erow) + Epub.
```

In particular **do not multiply `Epub` by another resolvent factor**. It is a direct distance to the ideal reconstructed vector, not an original-equation residual. Rounded isolate errors are correctly covered by this same triangle inequality without recomputing dangling mass from the rounded candidate.

All interval operations, absolute distances, weights, ledger additions and the final comparison must be justified, not merely the last rounding operation. Rounding a signed difference upward and then taking its absolute value can round its magnitude downward. Exact integer degrees must remain exact in the endpoint products, including above `2^53`. Nonfinite bounds, uncertain positive denominators, invalid personalization and exhausted arithmetic range must fail closed. A serialized certificate bound must preserve its upper-bound direction, and its tolerance must have a specified exact interpretation.

## Closest Work And Stronger Controls

### Same-State Summation Control

Rump, *Error Estimation of Floating-Point Summation and Dot Product*, BIT 52 (2012), pp. 201-220: [author manuscript](https://www.tuhh.de/ti3/paper/rump/altRu11.pdf), equation (3.3), Theorem 3.5 and Corollary 3.7. Equation (3.3) already gives the gamma-to-computed-sum conversion. Theorem 3.5 yields, for these nonnegative sequential sums,

```text
|z_f-Q_f| <= (s_f-1)*u*ufp(z_f),
ufp(z) = largest power of two <= z; ufp(0)=0.
```

Replace `Escatter` by the upward sum of these bounds times `s_f`. This uses the same `z`, cardinality stream and scalar ledger; no extra factor array. The mathematical bound has no summand-count restriction, but bound evaluation still needs correct integer/range handling. Implement `ufp` by exact exponent decoding, not a potentially misrounded logarithm. This dominates the proposed term on its admitted domain. The source's claim that `gamma_f*z_f` is a wrong shortcut needs qualification: it is not justified by its displayed inversion, but is safe here under this stronger theorem. Positivity remains essential to using `z_f` as the absolute-sum accumulator.

### Other Relevant Primary Work

- **Ogita, Rump and Oishi, 2005, *Accurate Sum and Dot Product*.** [Primary manuscript](https://www.tuhh.de/ti3/paper/rump/OgRuOi05.pdf), Algorithm 4.1 (`Sum2s`), equation (4.2), Corollary 4.7. Error-free transformations and computable error estimates predate this proposal. From the displayed recurrence, a single stream needs only running sum/correction state; interleaved factor streams require corresponding per-factor state or reordering. A useful comparator is validated compensated scatter with all extra arrays charged. Compensation alone is not a certificate: validate its own rounding, final conversion and underflow terms.
- **Ozaki, Ogita, Miyajima, Oishi and Rump, 2007, *A Method of Obtaining Verified Solutions for Linear Systems Suited for Java*.** [Primary paper](https://ogilab.w.waseda.jp/ogita/math/doc/2007_OzOgMiOiRu.pdf), Algorithms 1-2 and Section 4.2. It combines dot-product enclosures with residual-based forward-error verification and explicitly addresses overestimation from coarse arithmetic bounds. It is not this incidence/output schedule and uses approximate-inverse machinery; the overlap is validated residual evaluation, not identical storage complexity. Replacing an N/F-vector of error radii with a weighted scalar does not invent residual verification.
- **Neal, 2015, *Fast Exact Summation Using Small and Large Superaccumulators*.** [Author preprint](https://www.cs.toronto.edu/~radford/ftp/xsum.pdf), small-superaccumulator section, pp. 4-9. Exact binary64 summation with fixed-format accumulator storage is established; the small accumulator has 67 64-bit chunks plus bookkeeping. It is a relevant control after grouping contributions by factor, but one such accumulator per interleaved factor is not a free 8-byte replacement. Exact accumulation also does not remove row reconstruction or publication error. Charge sorting, scratch and any final rounding enclosure.

These primary texts were read at the cited mathematical/algorithmic locations. This is a targeted closest-art review, not an exhaustive priority search. Their performance figures are not transferred to this workload.

### Required Comparison Design

1. **Freeze the input `h` for certificate-only comparisons.** Compare the proposal, the same-state control above, and direct certification on the same graph, row order and emitted bytes. Report bounds and admission/refusal, not just elapsed time on successful cases. If a method changes output bits, make that a separate full-workflow comparison.
2. **Preserve the direct actual-file certifier.** Add a factor-blocked version as a bounded-memory control, charging replay of source/output, h access, decoder state and scratch. The reference shares source trust unless the builder is checked independently.
3. **Compare validated compensated and grouped exact accumulation.** Price added per-factor state or external reordering explicitly. Use these to distinguish necessary resource costs from the proposed bound's conservatism. A simple upward local-roundoff ledger is another valid design candidate, but factor weights must be available per update or conservatively replaced by an admitted maximum.
4. **Share the publisher across compatible solvers.** Ordinary reduced CG, dense reuse, relaxed reduced power and the candidate must all receive it. Do not infer solver superiority from a shared certifier improvement. Converting a vertex-state solver to `h` can change the candidate and costs work; charge it explicitly rather than claiming universal zero-cost compatibility.
5. **Separate certificate and lifecycle evidence.** Report successful/failed enclosures, required extra iterations, retries and fallback costs, as well as live buffers, row high-water mark, scalar precision/range, source/output passes and hash/readback traffic. `16F` is array payload, not a process or physical-memory cap. This review executes none of those performance comparisons.

## Independent Exact Checks

### One Native Fixture

Use five vertices and ordered factors `({0,1,2},{0,1},{0,1})`, uniform `p`, `a=17/20`, and the exact binary64 state `h=(-1/32,1/4,1/8)`. Vertices 3 and 4 are isolated; the last two columns repeat. Independently expanding adjacency and solving the original rational stationary system gives

```text
s = (3,2,2), q = (3,3,1,0,0), d = (4,4,2,0,0)
pZ = 2/5, bEffective_i = 1/22
y_active = (2377/46112,2377/46112,7/1056)
x* = (380/1067,380/1067,210/1067,1/22,1/22).
```

Scatter `float(y_i)` sequentially in vertex order, emit `float(d_i)*float(y_i)` on active vertices and `float(1/22)` on isolates. Pack/unpack little-endian binary64 before measuring error. Exact arithmetic then gives

```text
actual L1 error = 3065564741773883139/6341068275337658368
                ~= 0.4834461022438121
Erow           = 68229/1661359888138466492416
Epub           = 64901/2492039832207699738624
Rhat           = 54802650566232925/72057594037927936
Efinal         ~= 4.3097241498959065

weighted actual scatter error = 21/1152921504606846976
weighted same-state control   = 5/72057594037927936.
```

This intentionally poor signed candidate demonstrates validity, not useful tolerance admission. The check used point intervals `[y_i,y_i]` and exact ledgers to isolate the algebra. It does **not** validate finite-precision interval construction, a Decimal context, transaction behavior or any repository implementation.

### Failure Witnesses And Boundary Cases

- **Missing publication error:** three isolated vertices, uniform personalization, any admitted damping. Exact scores are `1/3`. Three emitted binary64 approximations have total exact L1 error `2^-54`, despite no factor residual. Omitting `Epub` would certify zero.
- **Missing row weights:** perturb only row 0 of the native fixture by positive amount `delta`. Weighted factor error is exactly `(3+2+2)*delta=7*delta`; degree-only, membership-only and unweighted charges give `4*delta`, `3*delta` and `delta`. Equation (2)'s multiplicity cannot be dropped.
- **Signed-scatter misuse:** sequential binary64 summation of `[2^53,1,-2^53]` gives `z=0` but exact sum `Q=1`. Any positive-sum error formula proportional to this `z` reports zero. All three nonzero operands are normal; normality alone is insufficient.
- **False source metadata:** two vertices, one factor `{0,1}`, uniform `p`, `a=2/3`. Replace the true degrees `(1,1)` by `(2,2)` while keeping `q=(1,1)` and `s=2`. With dyadic `h=1/4`, the forged reconstruction gives `y=(1/8,1/8)`, `T=h`, emitted scores `(1/4,1/4)`, and zero row/publication errors. The proposal's scatter allowance gives only `Efinal=1/4503599627370494`; actual PageRank is `(1/2,1/2)` and actual error is `1/2`. This violates the source contract, not theorem (4), and precisely demonstrates why hashes alone cannot establish it. The check `B*s=d+q` already rejects this forgery.
- **Harmless conservative refusal:** two vertices with one factor, uniform `p`, `a=1/2`, `h=1`. Everything reconstructs and sums exactly to `(1/2,1/2)`, but the proposed `Efinal` is `1/2251799813685247`, not zero. A stricter tolerance can reject an exact answer; this is not an incorrect bound.
- **Negative exact row rounding to zero:** on that two-vertex source, set `h=-1/2` and exact rational `a=(1+2^-1075)/(2-2^-1075)`. Then both exact rows equal `-2^-1076` and round to negative zero. The scatter is admitted as zero, while `e_i=2^-1076` must still be charged. This extends beyond the fixed `17/20` experiment and probes the general rational-damping statement only.

### Reproduction

The following self-contained arithmetic witness imports no repository modules and writes no files. Its stationary target is checked directly against the expanded original operator, independent of the reduced residual. Printed decimal approximations are for readability; every inequality is compared as an exact rational. It is a bounded mathematical witness, not a duplicate implementation test suite.

```python
from fractions import Fraction as F
from struct import pack, unpack
from math import frexp
import sys

assert sys.float_info.radix == 2 and sys.float_info.mant_dig == 53
u = F(1, 2**53)
groups = [(0, 1, 2), (0, 1), (0, 1)]
n, a, p = 5, F(17, 20), F(1, 5)
h = [F(-1, 32), F(1, 4), F(1, 8)]
s = list(map(len, groups))
rows = [[f for f, g in enumerate(groups) if i in g] for i in range(n)]
q = list(map(len, rows))
A = [[F(sum(i in g and j in g for g in groups)) if i != j else F(0)
      for j in range(n)] for i in range(n)]
d = list(map(sum, A))
P = [[A[i][j]/d[j] if d[j] else p for j in range(n)] for i in range(n)]
xstar = [F(380, 1067), F(380, 1067), F(210, 1067), F(1, 22), F(1, 22)]
assert all(sum(P[i][j] for i in range(n)) == 1 for j in range(n))
assert all(xstar[i] == (1-a)*p+a*sum(P[i][j]*xstar[j] for j in range(n))
           for i in range(n))
pZ = sum(p for di in d if not di)
b = (1-a)*p/(1-a*pZ)
y = [(b+a*sum(h[f] for f in rows[i]))/(d[i]+a*q[i]) if d[i] else F(0)
     for i in range(n)]
X = [d[i]*y[i] if d[i] else b for i in range(n)]
yhat, z = list(map(float, y)), [0.0]*len(groups)
for i in range(n):
    assert yhat[i] >= 0 and (yhat[i] == 0 or yhat[i] >= sys.float_info.min)
    for f in rows[i]:
        z[f] = z[f] + yhat[i]
payload = b''.join(pack('<d', float(d[i])*yhat[i] if d[i] else float(b))
                   for i in range(n))
xhat = list(map(F, unpack('<5d', payload)))
T = [sum(y[i] for i in g) for g in groups]
Q = [sum(F(yhat[i]) for i in g) for g in groups]
r = [v-w for v, w in zip(T, h)]
R = [(1-a)*p+a*sum(P[i][j]*X[j] for j in range(n))-X[i] for i in range(n)]
assert R == [a*sum(r[f] for f in row) for row in rows]
Erow = sum((d[i]+q[i])*abs(F(yhat[i])-y[i]) for i in range(n))
Epub = sum(abs(v-w) for v, w in zip(xhat, X))
Rhat = sum(sf*abs(F(zf)-hf) for sf, zf, hf in zip(s, z, h))
Escatter = sum(sf*sf*u/(1-2*sf*u)*F(zf) for sf, zf in zip(s, z))
Erump = sum(sf*(sf-1)*u*F(2)**(frexp(zf)[1]-1) for sf, zf in zip(s, z))
Eactual = sum(sf*abs(F(zf)-v) for sf, zf, v in zip(s, z, Q))
assert sum(sf*abs(v-w) for sf, v, w in zip(s, Q, T)) <= Erow
assert Eactual <= Erump <= Escatter
Efinal = a/(1-a)*(Rhat+Escatter+Erow)+Epub
error = sum(abs(v-w) for v, w in zip(xstar, xhat))
assert error <= Efinal
assert error == F(3065564741773883139, 6341068275337658368)
print('native error, bound:', float(error), float(Efinal))
print('row, publication, residual:', Erow, Epub, Rhat)
print('scatter actual, stronger bound:', Eactual, Erump)
print('output bytes:', payload.hex())

third = F(unpack('<d', pack('<d', 1/3))[0])
assert 3*abs(F(1, 3)-third) == F(1, 2**54)
assert sum(s[f] for f in rows[0]) == d[0]+q[0] == 7
vals, rounded = [float(2**53), 1.0, -float(2**53)], 0.0
for v in vals:
    rounded += v
assert rounded == 0 and sum(map(F, vals)) == 1
a, bad_d, h0 = F(2, 3), F(2), F(1, 4)
y0 = ((1-a)/2+a*h0)/(bad_d+a)
assert y0 == F(1, 8) and 2*y0 == h0
bad_bound = a/(1-a)*2*(2*u)/(1-4*u)*h0
assert bad_bound == F(1, 4503599627370494)
assert 2*abs(F(1, 2)-bad_d*y0) == F(1, 2) > bad_bound
assert 4*u/(1-4*u) == F(1, 2251799813685247)
t = -F(1, 2**1076)
a = (1-2*t)/(2+2*t)
y0 = ((1-a)/2+a*F(-1, 2))/(1+a)
assert y0 == t and float(y0).hex() == '-0x0.0p+0'
print('six failure/boundary witnesses: assertions passed')
```

The native output buffer was exactly:

```text
b951b4698d64ca3fb951b4698d64ca3f279b6cb2c9268b3f46175d74d145a73f46175d74d145a73f
```

## Admission And Trust Controls

- **Validated immutable source:** establish `s=B^T*1`, `q=B*1`, `d=B*s-q`, factor-ID ranges, binary membership, pruning rules, exact counts and vertex universe. Bind all metadata to the same snapshot and semantic version. Hash equality establishes identity, not those identities. Charge validation and protect against replacement between validation and traversal; a pathname is not an immutable snapshot.
- **Independent arithmetic:** record exact `a`, personalization, exact input `h` bits, interval precision/range and scatter policy. All ledgers must remain nonnegative upper bounds. A finite upper bound meeting the exact tolerance is the only success path; arithmetic exceptions and unverifiable assumptions are refusals, not silent precision changes.
- **Serialized-value correspondence:** use the value decoded from the exact per-row encoded buffer for `eta_i`, or establish an equally strong encoder invariant. Fix endianness, row order, score count, handling of negative zero and prohibition of NaN/infinity. No subsequent normalization, clipping or text conversion is covered unless its output discrepancy is also charged.
- **Consistent publication:** handle partial writes and buffered close failures; bind the completed `N`-score artifact, bound, source, h and policy in one committed generation. Atomic replacement of only the score file does not make a separately replaced certificate atomic with it. Publish a manifest last or use an equivalent generation protocol. Durability and storage readback remain separately charged promises.
- **Independent oracle and failure handling:** retain actual-file verification during development and as an optional production mode; do not call the fused path an independent checker of its own encoder. A post-generation failure must preserve the prior published answer. Reserve fallback memory/scratch first or refuse; do not allocate reference interval arrays outside the stated cap.

## Qualified Position

The algebra merits implementation investigation with the controls above. The outstanding scientific question is whether the complete admitted workflow improves a useful memory/work/accuracy tradeoff against the stronger shared controls, especially the same-state summation replacement. Primary precedent already covers the numerical primitives, and the exact checks establish neither implementation correctness nor general resource benefit. No claim of project completion, global novelty or publication readiness follows from this review.

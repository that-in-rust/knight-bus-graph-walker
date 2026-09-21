# Mass-Correction Review

Date: 2026-09-20. Bounded independent mathematical challenge of `PageRank-Mass-Conservation-Correction.md`. No in-progress implementation, tests or benchmark driver were inspected. No public benchmark was run. Only this review was written; tiny independent checks used in-memory rational arithmetic.

## Verdict

The mass identity, positive construction of Cs, affine projection and exact-arithmetic convergence argument are correct. The projected iteration has exactly the desired fixed point. The same-sign certificate statement can be strengthened to equality with the actual L1 error.

Two controls are important before attributing practical benefit to projection:

1. **Over-relaxed reduced power:** `hNew=(1+a)*T(h)-a*h` has a proved contraction bound of a under the native `q<=d` contract, with two F buffers and no correction metadata.
2. **Final-only active-score normalization:** rescale the emitted scores using a scalar available from the ordinary row scan. This can completely remove a pure scaling error without changing the iteration or adding a normalization pass. It still needs the usual final-file certificate.

The source correctly warns against substituting the projected update difference for the raw residual. The exact counterexample below shows that this substitution can understate actual answer error, even when the current state is perfectly mass-feasible.

## Identity Audit

Use the source's active-row definitions, `0<a<1`, and write `bE=bEffective`, `M=(1-pZ)/(1-a*pZ)`, `r=g-Ch`. Isolates keep their exact stationary outputs. The binary-incidence contract supplies

```text
s = B^T 1,       B s = d+q,       K 1 = d+a*q
C = I-a B^T K^-1 B,              g = B^T K^-1 bE
sum_active bE = (1-a)*M.
```

Therefore

```text
u = C s = B^T[1-a*K^-1(d+q)]
        = (1-a)*B^T K^-1 d > 0,
G = s^T g = sum_active_i (d_i+q_i)*bE_i/K_i.
```

Strict positivity is coordinatewise because each retained factor contains active vertices. It fails as a strict statement at a=1 or for empty factors; both are outside the admitted solve. Positive sums avoid cancellation in constructing u, but do not make floating evaluation exact.

The two intermediate identities in the source are valid:

```text
M-d^T K^-1 bE = a/(1-a)*s^T g
a*d^T K^-1 B h = a/(1-a)*u^T h.
```

Subtracting gives, for every h, including signed states,

```text
M-1^T X(h) = a/(1-a)*s^T r.
```

This is also the full-vector mass deficit when exact isolate scores are included. For a rounded output, use its actual mass and original residual; the identity is not a substitute for the final-file certificate.

### Same-Sign Equality

With exact reconstruction, the original residual is `R=a B r` on active vertices and zero on isolates. If all entries of r are nonnegative, then R is nonnegative, and the nonnegative PageRank resolvent gives `x*-X=(I-aP)^-1 R >= 0`. The opposite sign case is identical after negation. Consequently,

```text
||X-x*||_1 = |1^T X-1|
           = a/(1-a)*sum_f s_f*abs(r_f)
```

for the full candidate with exact isolates. Thus the certificate is the exact L1 error in that regime, not just an unavoidable lower bound. This does **not** establish that the spatial error is a scalar multiple of x*, of s in factor space, or of one eigenvector. A same-sign residual diagnoses mass-controlled error, not necessarily a single removable spectral mode. The reported natural-source observations were not independently replayed here.

## Projection Audit

Let `E=s^T C s>0` and `Pi=I-s*u^T/E`. Then

```text
Project(h) = Pi*h + s*G/E
u^T Project(h) = G
Pi^2 = Pi,       Pi^T C = C Pi.
```

Pi is the C-orthogonal projection onto `u^T v=0`; its nullspace is `span(s)`. For `T(h)=h+r=g+(I-C)h`, the projected error iteration is `eNew=Pi*(I-C)*e`. Since C commutes with `I-C`, whose spectrum is in `[0,beta]`,

```text
||eNew||_C <= beta*||e||_C,       beta<1.
```

There are **no additional fixed points**. Every output of Project lies in the feasible affine hyperplane. At a fixed point, therefore, `s^T r=0` and `Pi*r=0`. The latter makes `r=t*s`; the former implies `t*s^T s=0`, hence r=0. Conversely, the exact solution is fixed. An initially infeasible state becomes feasible after one projected update, although explicit initial correction is appropriate for the intended invariant.

This proves convergence, not faster wall time, positivity, or a sharper general beta. Global mass feasibility also does not repair incorrect masses of individual disconnected components. At F=0 or a=0, bypass these formulas as prescribed. If M=0, all active scores are zero and the known isolate solution is already sufficient.

### Projected Difference Counterexample

For an exactly feasible h, set `delta=Project(T(h))-h`. Then

```text
delta = Pi*r
r = delta - s*(s^T delta)/(s^T s).
```

Thus zero delta does imply zero residual on the invariant hyperplane, but delta is not r and their certificate norms need not agree. A factor-two weighted bound follows from this recovery formula; the unmodified residual bound does not. In finite precision the recovery also needs a feasibility-error allowance. Keeping the raw residual is simpler.

Exact example: disjoint groups `{0,1}` and `{2,3,4}`, a=1/2, p uniform over five vertices. Then

```text
s = (2,3),        C = diag(1/3,2/5)
g = (2/15,3/25),  u = (2/3,6/5),    E = 74/15
h = (31/100,7/20)
r = (3/100,-1/50)
X(h) = (17/100,17/100,11/50,11/50,11/50)
x* = (1/5,1/5,1/5,1/5,1/5).
```

The candidate has mass one. Its actual L1 error and correct raw certificate are both `3/25=0.12`. But `delta=(117/3700,-65/3700)`, and treating it as r yields `429/3700`, strictly less than `3/25`. This falsifies that stopping substitution, not the proposed raw-residual algorithm.

### Equal Sizes And Components

When every group has size s0, `Cs=(1-betaSize)*s`, so projection removes that eigenvector exactly. Separate factor components can supply multiple copies of the same eigenvalue; one global correction removes only one linear combination. If components have different group sizes, their slow eigenvalues can differ, making the global correction still less like exact removal of a single eigenmode.

The initialization caveat can be sharpened. With no isolate personalization mass and `h0=B^T D^-1 p`, equal group sizes imply exact mass feasibility initially, and the raw iteration preserves it. Projection then does nothing in exact arithmetic. Moreover, the ordinary warm start already has the correct mass in each component, so it avoids each component's uniform-size mass direction, not merely the global one. With isolates, the initial active vector must first have the correct component masses, for example `x0_active=p_active/(1-a*pZ)`; using unadjusted p does not generally have this property.

## Simpler Controls

### Relaxed Reduced Power

The previous native review proved `lambda(I-C) in [0,beta]` with `beta<=2a/(1+a)`. Therefore ordinary relaxed Richardson with `omega=1+a` gives

```text
hNew = h + omega*(g-C h) = (1+a)*T(h)-a*h
lambda(I-omega*C) = (1+a)*lambda(I-C)-a in [-a,a]
||eNew||_C <= a*||e||_C.
```

It has the same unique solution and uses the same two arrays: gather raw T(h), check its difference from h, then transform the new buffer before swapping. No u, G, E, projection initialization, or correction-metadata scans are needed. It may produce signed iterates; a positivity-preserving interpretation must not be assumed.

With an admitted tighter beta, the standard interval-optimal constant relaxation is

```text
omega = 2/(2-beta)
||eNew||_C <= [beta/(2-beta)]*||e||_C.
```

For the native contract this bound is at most a. These are C-norm bounds, not an unmodified L1 successive-difference certificate; keep the raw residual/final-file checks. The stronger guarantee uses SPD and the proved spectral interval, not merely positivity of entries. Rounded coefficient/operator errors are not covered by the exact proof.

The same relaxation can be combined with projection: `Project(h+omega*r)` inherits the same C-norm upper bound because Pi is nonexpansive. None of these bounds promises a better particular run than projected unrelaxed power or CG. This is nevertheless a necessary inexpensive control against the source's weaker universal beta argument.

### Final-Only Normalization

Let `xA=X(h)`, `mu=1^T xA`, `deltaM=M-mu`. For `M>0` and `mu>0`, emit

```text
xNormalized_active = (M/mu)*xA
xNormalized_isolates = exact stationary isolate scores.
```

Do not normalize the full vector and thereby disturb already-correct isolates. If a probability output is required, also require nonnegative active scores. Scaling h alone is not equivalent because X is affine.

Writing `RA=bE+a*A*Dinv*xA-xA=a*B*r`, the normalized active residual is exactly

```text
RNormalized = (M/mu)*RA - (deltaM/mu)*bE.
```

This is not generally zero. For one two-vertex group, a=1/2, `p=(3/4,1/4)`, and h=0, raw X is `(1/4,1/12)`. Normalization gives p, whereas the true answer is `(7/12,5/12)`: an L1 error of 1/3 remains. In contrast, for the same group with uniform p and h=0, `X=p/3`; normalization immediately recovers the exact answer. It can therefore remove the entire apparent mass bottleneck in some cases, but same-sign r alone is insufficient to predict that outcome.

Importantly, mu can be accumulated while the ordinary fused row scan reconstructs X(h), or derived from the exact mass identity. Once known, scaling can happen during the normal output emission, followed by the already-required final-file certificate. There is **no inherent extra normalization pass or extra certificate relative to publishing another candidate**. Trying many early candidates still costs their output/certificate attempts. Assess normalized candidates at declared checkpoints; do not dismiss this control solely because normalization changes the candidate. A final-only Project(h) correction is another cheap ablation against projecting every update.

## Storage Schedule

The claimed two-F-array changing state is feasible. Stream static `(s_f,u_f)` records while retaining old h and raw next. The first factor pass can compute `u^T next` and the weighted **raw** residual screen. After obtaining the scalar correction, a second metadata pass updates next in place; only then swap. No stored g or residual vector is necessary. Initialization requires its own correction and the query scalar G. E is graph-and-damping dependent and reusable across queries.

This is `16F` bytes of f64 changing payload, not total RAM. Resident s/u adds two F arrays; a streamed encoding instead pays metadata reads and buffering. Building u may itself require an F accumulator or external reduction. Both metadata passes also access iterate buffers. Output/certification memory is separate, and no claim of lower peak RAM follows from the changing-state count alone.

For numerical admission, u and E must not silently underflow or lose positivity. A simple equivalent scaling removes the common factor `1-a`: store `ubar=B^T K^-1 d`, use `Gbar=G/(1-a)`, and replace `(u,G,E)` by `(ubar,Gbar,s^T ubar)` in Project. Compute Gbar directly from positive terms rather than dividing an already-underflowed G. This reduces one avoidable scale problem, not summation/feasibility error generally. Static metadata must match the exact graph generation and damping.

## Precedent And Evidence

I inspected the author-hosted primary [Nabben and Vuik, *Domain Decomposition Methods and Deflated Krylov Subspace Iterations*, ECCOMAS CFD 2006, Section 2, printed p. 3](https://diamhomes.ewi.tudelft.nl/~kvuik/papers/Nab06aV.pdf#page=3). Equations (2)-(4) give `P_D=I-AZ(Z^T A Z)^-1 Z^T`, the coarse solution `Z(Z^T A Z)^-1 Z^T b`, and the deflated system. Substituting `A=C`, `Z=s`, `b=g` gives `P_D^T=Pi` and coarse term `sG/E`: precisely the proposed affine correction. The source uses deflated CG; the present projected Richardson schedule is a specialization, not its claimed new invention. Extracted equation text was readable; screenshot retrieval timed out, so no visual audit is claimed.

Independent in-memory Fraction checks covered 24 fixtures: six small incidence families, two rational damping values, and uniform/nonuniform personalization. They included overlapping, disconnected, duplicate-column and isolated-vertex cases. Across 96 signed candidate states, exact assertions checked the mass identity, positive Cs formula, projection feasibility, projected-difference recovery and C-energy convergence bounds for projected and optimally relaxed steps. Same-sign L1 equality and the two explicit counterexamples above were also checked. All assertions passed. This is finite algebra validation, not implementation validation or performance evidence; no test artifact was written.

**Conclusion:** useful, correctly derived native-incidence mass control built from known coarse projection. Its practical distinctness remains a question for equal-cost comparisons against relaxed power and final-only corrections. No global novelty claim is justified, and this does not establish another graph-innovation family. The review stops here.

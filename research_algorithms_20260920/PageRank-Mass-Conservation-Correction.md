# Mass-Consistent Factor Iteration After A Natural Failure

Date: 2026-09-20. A01 follow-on. The derivation now has a [completed independent mathematical review](PageRank-Mass-Correction-Review.md) and a [separate implementation/session experiment](PageRank-Mass-Correction-Experiment.md). The ordinary deflation foundation is explicitly credited below. This is a specialized candidate, not a claim of another novel algorithm. The original diagnostic table belongs to the earlier unprojected solve.

## Premise Check

In the [native-incidence experiment](PageRank-Native-Incidence-Evidence.md), reduced power requires 108-116 checks while fused ordinary power requires 17-18 updates on the same data. Every published answer is certified. The factor-only method is not merely waiting on a needlessly loose certificate: for the uniform query its late factor residual is componentwise positive, and the residual bound equals the missing total probability mass to displayed floating precision.

**Investigate a mass-consistent representation of the iteration before designing more storage variants.** The small q/d ratio protects a worst-case contraction bound, but it does not prevent a useful ordinary initialization from acquiring a slow mass mode under diagonal splitting.

## Expert Lenses

Numerical analysis asks which equation the iteration fails to preserve. Storage design asks whether its correction costs another graph pass or additional changing vectors. The skeptical perspective asks whether the fix is ordinary deflation and whether it actually beats CG. Product accounting asks whether any improvement survives preparation, output and certification.

## Candidate Approaches

1. Replace the conservative residual bound. This cannot remove the observed same-sign mass deficit; preserve it as a correct stopping control.
2. Normalize the final answer. That changes the candidate and requires certification again; it is not generally equivalent to solving the original equation sooner.
3. Normalize every iteration. This creates a nonlinear iteration whose convergence needs its own proof. Do not assume PageRank's ordinary contraction automatically applies.
4. Use the known mass equation as an exact coarse correction in the SPD factor system. This has a direct linear convergence argument and a one-pass graph schedule, but inherits standard projection/deflation machinery.

## Chosen Thesis

Test a **mass-consistent, two-factor-buffer Richardson schedule** with paid static factor metadata. The potentially useful work is choosing a source-derived coarse vector, encoding its correction without extra entity passes, and measuring the resulting full-cost frontier. Neither the projection formula nor its generic convergence is new numerical analysis.

## 1. Exact Mass-Residual Identity

Use the symmetric binary-incidence contract from the native experiment. All matrices in the following equations are restricted to active vertices; isolates retain their separately derived exact stationary contribution. Let

```text
s = B^T 1                          factor cardinalities
q = B 1
d = B s - q
K = D + a Q
C = I - a B^T K^-1 B
g = B^T K^-1 bEffective
X(h) = D K^-1 (bEffective + a B h)
r(h) = g - C h
```

Let M be the true active probability mass: `M=(1-pZ)/(1-a*pZ)`. Then `sum_i bEffective_i=(1-a)M` on active vertices. The identity

```text
C s = (1-a) B^T K^-1 d
```

follows because `B s=d+q` and `K*1=d+a*q`. In particular Cs is componentwise positive for each retained nonempty factor; compute it by positive sums, not by subtracting nearly equal vectors.

Now expand the mass of X(h). Since

```text
M - d^T K^-1 bEffective = a/(1-a) * s^T g,
a d^T K^-1 B h          = a/(1-a) * (C s)^T h,
```

we obtain

```text
M - 1^T X(h) = a/(1-a) * s^T r(h).                 (1)
```

The full candidate, with exact isolate scores added, has the same deficit relative to one. This is an exact-arithmetic identity, not an assumption that the rounded output already has mass one.

The ordinary factor certificate is `a/(1-a) sum_f s_f |r_f|`. If all r_f have the same sign, that bound equals the absolute candidate mass error by (1). A normalized true PageRank vector is at least that far away in L1. In this regime the certificate cannot be tightened below the mass deficit without changing the candidate.

The independent review strengthens this to equality with actual L1 error: same-sign r gives same-sign original residual aBr, and the PageRank resolvent is nonnegative. However, this does not imply that error is a pure scaling of x*, or a single eigenvector. The phrase mass mode is a diagnostic shorthand, not a proved one-eigenmode characterization.

## 2. Coarse Correction

For F>0, define

```text
u = C s
E = s^T u > 0
G = s^T g
Project(h) = h + s * (G - u^T h)/E.                 (2)
```

Because C is SPD, E is strictly positive. The corrected state satisfies `s^T(g-C*Project(h))=0`, so its exact reconstructed candidate has the required active mass. This is not a promise that each coordinate is nonnegative. Signed intermediate states are permitted by the original residual certificate; silent clipping is not.

The projected iteration is

```text
h0 = Project(B^T D^-1 p)

repeat:
    next = g + (I-C) h            one fused entity-row scan
    residual = next - h          evaluate without mutating h
    if original certificate screen is small:
        reconstruct X(h), publish provisionally, certify actual bytes
        return only if final certificate accepts
    next = Project(next)         factor metadata scans, no entity scan
    swap(h, next)
```

As in the previous solver, the check applies to h and its raw fixed-point residual. **Do not test `Project(next)-h` as if it were `g-C h`.** Projection can hide a coarse residual; using only the corrected update difference without the appropriate proof would change the stopping contract.

At F=0, bypass E and return the isolated-graph answer. The present derivation assumes 0<a<1. At a=0, return p without applying these divided formulas.

## 3. Convergence Argument

Let `Pi=I-s*u^T/E`. This is the C-orthogonal projector onto `{v : s^T C v=0}`. Therefore `||Pi v||_C<=||v||_C`. The true solution satisfies Project(h*)=h*. If e=h-h*, one step gives

```text
eNext = Pi (I-C) e.
```

The eigenvalues of `I-C` lie in [0,beta], and C commutes with I-C. Hence its induced C-norm is at most beta. Thus

```text
||eNext||_C <= beta ||e||_C < ||e||_C   for nonzero e.
```

This supplies convergence in exact arithmetic without assuming that a normalization heuristic behaves well. It does NOT prove fewer observed steps, a sharper universal beta, a rounded mass invariant, or lower latency than CG.

When all retained groups have the same size s0, `C s=(1-betaSize)s`, where `betaSize=a*s0/(s0-1+a)`. The projection removes that eigenvector exactly. The remaining contraction is governed by the remaining eigenvalues. If the factor graph has several disconnected components, the top eigenvalue can have multiplicity; removing one global vector does not remove every component's slow direction. Componentwise coarse corrections are a possible extension, with paid component discovery, metadata and scalar state. They are not part of the current design or measurements.

There is also a limitation: under equal group sizes, a mass-correct ordinary initialization may already avoid this direction. That case cannot be used to manufacture a measured gain for correction. The natural failure instead has unequal group sizes.

The review further notes that with isolate personalization, unadjusted `h0=B^T D^-1 p` need not have the correct active component masses even for equal group sizes. The stronger no-correction-needed statement applies when the initial active mass is correct, for example starting from `p_active/(1-a*pZ)`. It also supplies the cheaper relaxed iteration `hNew=(1+a)T(h)-a*h`, with exact C-norm contraction at most a under q<=d. An admitted tighter beta permits the standard optimal constant `omega=2/(2-beta)`. These ordinary controls are now included in the follow-on experiment; a better bound need not mean fewer measured steps.

## 4. Bounded Execution Recipe

Build factor records `(s_f,u_f)` with `u_f=(1-a)*sum_{i in f} d_i/(d_i+a*q_i)`. For a fixed graph and damping factor they are query-independent. Compute the query scalar

```text
G = sum_active_i (d_i+q_i)*bEffective_i/(d_i+a*q_i).
```

That scalar can be accumulated in the query initialization scan; it does not require retaining all of g. The builder may initially accumulate F-sized metadata arrays, or use an externally sorted factor reduction if necessary. Charge whichever is actually implemented.

After the entity pass generates next, one factor-record pass computes `u^T next` and the weighted residual bound. A second factor-record pass applies the now-known scalar correction. Both old and new factor vectors remain available; no N-vector or dense C is needed. The initial state needs its own correction pass. A float metadata implementation must retain a positive denominator or fail explicitly, and the final interval certificate is still mandatory.

| Resource | Derived cost; implementation evidence is in the linked experiment |
| --- | --- |
| Changing solve payload | Two F-sized f64 arrays, 16F bytes |
| Static correction metadata | One cardinality and one coefficient per factor; a u64/f64 encoding would use 16F bytes before headers |
| Per-update graph access | One entity-row scan, as in reduced power |
| Extra factor access | Approximately two sequential factor-metadata scans per update, plus initial correction |
| Resident-metadata option | Adds two F-sized arrays; can erase the selected-state advantage when F is close to N |
| Output and certificate | Same complete reconstruction and two interval-certificate scans as before |
| Numerical guarantees | Exact-arithmetic convergence argument; the implemented follow-up additionally certifies actual rounded output with the original-operator interval check |

This corrects a specific bad iteration without promising a better overall memory frontier. The file-backed ordinary power control also has only O(F) resident iterative state and needs no correction metadata; it pays changing vertex-file traffic instead. CG may converge in fewer passes while using more F vectors. Reused Cholesky may win many-query sessions. All remain required competitors.

## 5. Executed Diagnostic

The uniform native-source diagnostic produced these floating observations. Index k means X(h_k), with h_0 gathered from uniform p. This diagnostic was untimed and separate from the frozen public receipt.

| k | Mass deficit `1-sum X(h_k)` | Weighted factor error bound | Positive / negative residual coordinates |
| ---: | ---: | ---: | --- |
| 0 | 3.5162718e-4 | 1.6264988 | 791 / 152 |
| 5 | 1.6449251e-4 | 5.3980914e-4 | 465 / 478 |
| 10 | 7.3238268e-5 | 7.3238268e-5 | 943 / 0 |
| 20 | 1.4526246e-5 | 1.4526246e-5 | 943 / 0 |
| 30 | 2.8811699e-6 | 2.8811699e-6 | 943 / 0 |

This supports the mass-deficit diagnosis, not the speed of the correction. The earlier experiment's published final results still satisfy their independent interval certificate. Measurements of the new implementation belong only to the separate follow-on receipt.

## 6. Prior-Art Boundary

Nabben and Vuik, *Domain Decomposition Methods and Deflated Krylov Subspace Iterations*, ECCOMAS CFD 2006, [author-hosted primary PDF](https://diamhomes.ewi.tudelft.nl/~kvuik/papers/Nab06aV.pdf), Section 2, equations (2)-(4), explicitly gives the projector `P_D=I-A Z (Z^T A Z)^-1 Z^T` and the separately computable coarse solution. Setting their A=C and Z=s makes the transpose projector our Pi and supplies exactly the coarse term in (2). Extracted PDF text on printed p. 3 was inspected; no visual equation audit is claimed.

Accordingly, this is **ordinary rank-one coarse correction with a source-derived vector**, not a newly invented deflation method. Equation (1), its native-incidence interpretation and a paid two-buffer execution path are the specific deductions developed here. Their standalone research novelty has not been established. A direct ordinary application of known deflation can reproduce the correction.

The literature context for capacitance PageRank, clique-projection semantics and CG is in the [completed native-incidence review](PageRank-Native-Incidence-Review.md). Do not rename this method and treat failure to find that name as a priority result.

## Final Synthesis And Next Falsifiers

The natural experiment exposes a concrete design problem: state elimination can destroy an invariant that made the original iteration fast. Preserving an inexpensive global equation may matter more than another storage encoding. That is a useful architecture lesson even if the remedy is established numerical analysis.

The streamed correction and cheaper controls are now implemented in the separate experiment. The completed independent challenge supports the derivation and contributes counterexamples and relaxed/normalized controls; it is not a code audit. Exact small-instance tests and paid sessions are tracked in [the experiment report](PageRank-Mass-Correction-Experiment.md). Preserve the original eight-mode receipt rather than overwriting its negative finding. Mathematical support and an implementation do not establish global novelty or performance outside the measured scope.

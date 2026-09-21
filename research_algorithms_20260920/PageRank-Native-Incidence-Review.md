# Native-Incidence PageRank: Independent Review

Date: 2026-09-20. Bounded A01 research sidecar. Read the supplied Factor-Defect-PageRank and PageRank-Prior-Art-Comparison documents. This review uses mathematical inspection and bounded primary-source retrieval only: no benchmarks, numerical probes, source/test changes, or independent research continuation. The only file written is this review. The lead's natural-source implementation was not inspected or validated.

## Premise Check

**Verdict: the proposed algebra, isolate elimination, spectral enclosure, and condition-number bound are valid under the precise contract below. They are not evidence of a new graph algorithm family.** The reduced system is a standard capacitance/Schur specialization; power, Jacobi, and CG are established solvers. Native incidence is a useful source/representation case for A01, not an additional independently established innovation toward the seven-family goal.

The strongest useful hypothesis is narrower: **a source-faithful, bounded-memory execution and certification pipeline can complete exact clique-projection PageRank jobs more economically than equally factor-aware incumbents.** No measured advantage is established here.

Critical qualifications:

- Remove isolated rows before applying `K^-1`, but preserve their IDs and scores in the full output.
- Binary membership means unique `(entity, group)` records. Distinct groups with identical membership still contribute separately unless their multiplicity is retained as a weight.
- The target is the multiplicity-weighted, loop-free clique projection, not a Boolean projection, normalized hyperedge walk, nonlinear hypergraph PageRank, or a new higher-order ranking semantics.
- After singleton pruning, `q_i <= d_i` on active vertices. The general proposal's arbitrarily large cancellation ratio is unavailable in this native case.
- The strong ordinary baseline needs one active-entity vector plus two factor vectors, not necessarily two entity vectors. It can also externalize its entity vector at an I/O cost.
- Directed-rounding certification is conceptually valid, but merely using Decimal does not implement interval arithmetic correctly.

## Expert Lenses

The checks below separate four questions: exact graph semantics, numerical linear algebra, external-memory execution, and skeptical attribution. Mathematical deductions in this review are not reported as executed experiments or as theorems newly established in the literature.

## Mathematics

### Contract And Pruning

Let `B` be the retained binary `N x F` incidence matrix; `s_f = sum_i B_if >= 2`; `q_i = sum_f B_if`; and `d_i = sum_{f:i in f}(s_f-1)`. Then

```text
Q = diag(q)
A = B B^T - Q
A_ii = 0
A_ij = number of retained groups containing both i and j, i != j
A 1 = d
```

A singleton contributes `e_i e_i^T` to `BB^T` and the same diagonal contribution to Q, hence contributes zero to A. Removing it and updating q preserves A and d. Empty groups also contribute nothing. Preserve the vertex universe separately: rebuilding vertices only from surviving memberships silently drops some isolates and changes p.

After pruning, `d_i=0` iff `q_i=0` iff row i of B is zero. Symmetry makes these vertices genuinely isolated, with no incoming edges. This equivalence does not extend to the directed case in the main proposal.

For a source vertex j, the present walk can be interpreted as choosing an incident group f with probability `(s_f-1)/d_j`, then another member uniformly. Choosing incident groups uniformly instead generally defines a different operator. Large-group weighting is a semantic choice, not just an encoding detail.

### Isolate Elimination

Take `0 <= a < 1`, a nonnegative rational distribution p with exact sum one, `Dinv_ii=1/d_i` on active vertices and zero otherwise, and z the isolate indicator. The original problem is

```text
P = A Dinv + p z^T
b = (1-a) p
x* = b + a P x*
pZ = z^T p
```

Writing `m = z^T x*`, the isolate equations give

```text
m = (1-a)*pZ + a*pZ*m
m = (1-a)*pZ / (1-a*pZ)
bEffective = ((1-a)/(1-a*pZ)) * p
x*_i = bEffective_i                         on isolates
x*_active = bEffective_active + a*A_active*Dinv_active*x*_active
```

The denominator is positive, including `pZ=1`. If all mass is on isolates, active scores are zero. If the graph is entirely edgeless, the answer is p. At `a=0`, return p. An empty universe needs an explicit empty-result convention because no probability distribution on it exists.

Do not normalize `bEffective` or the active result independently. Active mass is `(1-pZ)/(1-a*pZ)`, not generally one. The formula eliminates stationary dangling mass; it does not reproduce every transient iterate of ordinary full-graph power starting from p.

### Reduced SPD System

In this subsection B, D, Q and `bEffective` are restricted to active rows. Let

```text
y = D^-1 x
K = D + a Q
(K - a B B^T)y = bEffective
h = B^T y
g = B^T K^-1 bEffective
M = a B^T K^-1 B
C = I - M
C h = g
y = K^-1 (bEffective + a B h)
X(h) = D K^-1 (bEffective + a B h)
```

All active diagonal entries of K are positive. There is no `0/0` branch to evaluate on isolates: their outputs are handled separately.

For any entity vector v, groupwise Cauchy-Schwarz gives

```text
v^T B B^T v = sum_f (sum_{i in f} v_i)^2
              <= sum_f s_f * sum_{i in f} v_i^2
              = sum_i (d_i+q_i) v_i^2.
```

Thus `0 <= BB^T <= D+Q` in positive-semidefinite order. The nonzero eigenvalues of M equal those of `a K^-1/2 BB^T K^-1/2`, so

```text
betaRow = max_active_i a*(d_i+q_i)/(d_i+a*q_i)
0 <= lambda(M) <= betaRow < 1
1-betaRow <= lambda(C) <= 1.
```

In particular C is symmetric positive definite even if B has dependent or duplicate columns, or `F > N_active`. Redundant factor directions have C-eigenvalue one. Disconnectedness and zero entries in p do not invalidate SPD. For `F=0`, bypass the empty reduced solve and do not report its condition number.

For a retained minimum size `smin >= 2`,

```text
d_i >= (smin-1)*q_i
q_i/d_i <= 1/(smin-1)
betaRow <= betaSize = a*smin/(smin-1+a)
lambda(C) lies in [1-betaSize, 1]
kappa_2(C) <= 1/(1-betaRow) <= 1/(1-betaSize)
          = (smin-1+a)/((smin-1)*(1-a)).
```

The ratio function is increasing for `0<a<1`; at a=0 everything is trivial. This proves the requested bound, but **beta is a bound, not necessarily the spectral radius**, and the condition-number bound need not be attained. For one retained group C is scalar and has condition number one, even though the displayed upper bound is larger.

The size-based lower spectral endpoint is attainable: if all groups have size s, then `d=(s-1)q` and `M 1 = a*s/(s-1+a) * 1`. This is a symbolic check, not a numerical probe.

Consequences: singleton pruning already supplies `betaRow <= 2a/(1+a)`. The main proposal's release set `q_i>d_i` is empty here. A stricter release target might still trade state for contraction, but this experiment cannot demonstrate the need to cure unbounded native cancellation. At `a=0.85`, the size-two bound is `betaSize=34/37` and `kappa_2(C)<=37/3`; these are theoretical values, not observed convergence.

## Candidate Approaches

Let `L=nnz(B)` and retain the same exact semantic input for every method. The requested controls are appropriate, with the following definitions and caveats.

| Control | Exact update or solve | Work/state qualification |
| --- | --- | --- |
| Ordinary incidence-aware power | `h=B^T Dinv x`; `xNew=bEffective+a*(B h-Q Dinv x)`; gather `hNew=B^T Dinv xNew` | One active x array can be overwritten rowwise while old h remains fixed. Two F buffers. Isolates fixed analytically for this stationary-equivalent baseline. |
| Reduced power | `hNew=g+M h`; reconstruct `X(h)` rowwise and immediately gather `x_i/d_i` | Two F buffers; no entity iterate. Positive reconstruction for nonnegative h, but a different iteration from ordinary power. |
| Ordinary factor Jacobi | `m_f=M_ff=a*sum_{i in f}1/K_i`; `hNew_f=((g+M h)_f-m_f*h_f)/(1-m_f)` | Two F iterate buffers plus stored/streamed diagonal. Use the residual of C, not the Jacobi update difference, for stopping conversion. |
| Factor CG | Solve `C h=g`; apply `C v=v-a*B^T(K^-1(B v))` matrix-free | Standard SPD CG, not CG on normal equations. Typically h, residual, direction and product are simultaneous F arrays; RHS/preconditioner storage depends on implementation. |
| Dense control | Assemble C, solve by Cholesky, reconstruct | Appropriate when admitted F is small. Charge matrix, factorization/workspace, assembly and certification; do not exclude it to manufacture a factor-power win. |

Each matrix-free update/product can use `O(N_active+L+F)` arithmetic and one logical entity-row traversal. For ordinary power, finish using old `x_i` before overwriting it; never change old h while scanning. This is still synchronous power, not Gauss-Seidel. Independently rounded h may drift from the exact gather of x, which is another reason to certify the actual returned x.

Factor Jacobi converges under this contract. With factor-size weights `c_f=s_f`, `c^T M <= betaRow*c^T`. Since `m_f<=betaRow<1`, its nonnegative iteration matrix `diag(1-m)^-1*(M-diag(m))` contracts in weights `c_f*(1-m_f)`, with weighted column ratios at most `(betaRow-m_f)/(1-m_f)<=betaRow`. This is an ordinary splitting argument, not a new solver or a guarantee of fewer measured passes.

Reduced power's bound is generally worse than a, the ordinary active power L1 contraction. Different spectra, initialization and stopping bounds prevent a universal speed ordering. CG can have signed intermediate h or reconstructed scores; its recurrence residual is not a certified original-space error. Do not silently clip or normalize after certification.

For comparison, the ordinary active system also has an SPD form, `I-a*D^-1/2 A D^-1/2`, with eigenvalues in `[1-a,1+a]`. An ordinary preconditioned/degree-scaled CG control is therefore legitimate if the experiment makes claims beyond power-versus-power. For size-two groups, its standard condition-number bound equals the reduced bound above. Symmetry-enabled CG is not exclusive to factor elimination.

Use common damping, p, output contract and final certified L1 tolerance. Report initialization passes and initial residuals. Starting each method from its conventional warm start is defensible; treating `h0=B^T Dinv p` as producing exactly the same initial entity vector as `x0=p` is not. Permit zero-iteration success when the initial candidate is already certified.

## Numerical Certificate

**The proposed two-output-scan certificate is mathematically valid**, provided it evaluates the original operator and encloses every arithmetic operation. It certifies whatever solver produced the file; it need not trust that solver's reduced recurrence.

Let x mean exactly the real vector represented by the final immutable output file. For decimal-text output, decide whether the contract means those decimal rationals or values obtained after a consumer's binary64 parse. For binary64 output, decode the actual binary values exactly for certification. These contracts are not interchangeable.

1. **First scan:** merge output with validated entity rows. Set `Y_i` to an outward interval for `x_i/d_i` on active rows and `[0,0]` on isolates. Accumulate factor intervals `H_f=sum_{i in f}Y_i` and the isolate-mass interval `Z=sum_{d_i=0}x_i`. This requires F interval accumulators, but no N-sized interval vector.
2. **Second scan:** reread the same file and rows, recompute `Y_i`, and enclose the original residual

   ```text
   R_i = (1-a)*p_i + a*(sum_{f:i in f} H_f - q_i*Y_i + p_i*Z) - x_i.
   ```

   Accumulate upward `RUpper=sum_i max(abs(lower(R_i)),abs(upper(R_i)))` and return an upward enclosure of `RUpper/(1-a)`.

P is exactly column-stochastic, so the Neumann-series argument gives `||x-x*||_1 <= ||b+aPx-x||_1/(1-a)`. The bound applies to signed, unnormalized candidates too; nonnegativity and total mass may additionally be publication requirements. This is the established original-residual bound in [Gleich, Aside 2.3, PDF p. 3](https://arxiv.org/pdf/1407.5107#page=3).

### Required Numerical Safeguards

- **Exact source ledger:** group sizes, membership counts and degrees are exact integers before floats. Compute d as positive sums of `s_f-1`, not a floating subtraction of `sum s_f` and q. Validate the binary-membership and universe contract first; exact arithmetic on malformed records still certifies the wrong graph.
- **Exact parameter meaning:** verify p sums to one as rationals and pin the exact value intended by a. Outwardly enclose all conversions and rational divisions. Certifying a rounded p that no longer sums to one does not justify the stochastic `1/(1-a)` bound for the intended source.
- **Original dangling term:** use Z from the file, not the analytic stationary mass or an assumed zero. Rounded isolate outputs perturb every `p_i*Z` term. On isolates the residual is `(1-a)*p_i+a*p_i*Z-x_i`, not simply `bEffective_i-x_i` for an arbitrary rounded file.
- **Interval subtraction:** for `H=[l,u]` and `Y=[v,w]`, the enclosure of `H-q*Y` is `[roundDown(l-q*w), roundUp(u-q*v)]`. Running the whole subtractive formula once with all lower endpoints and once with all upper endpoints is wrong. Interval multiplication must handle signed candidates and uncertain parameters correctly.
- **Runtime discipline:** use floor/ceiling rounding, not toward-zero/away-from-zero as substitutes. Guard invalid operations, overflow, exponent limits and nonfinite results. Keep final absolute values, summation, division and tolerance comparison outward-safe. Python documents context-independent `copy_abs()`/`copy_negate()` and the distinction between exact float conversion and decimal-string conversion; arithmetic precision is context-controlled. [Python Decimal documentation](https://docs.python.org/3/library/decimal.html).
- **Final bytes:** normalize, clip, quantize or rescale before these scans, or certify again. If the certified bytes are exactly the published bytes, no separate output-rounding allowance is needed. If they are changed afterward, the certificate no longer covers them without a proved allowance.
- **Identity checks:** require every source vertex exactly once with the correct ID, including isolates; reject truncation, duplicate IDs and mismatched generations. Record parameter/source/output identities. A checksum binds bytes, not the correctness of the graph compiler.
- **Explicit failure:** insufficient precision or a wide enclosure means not certified. It does not prove the candidate inaccurate, and it must not be converted to success by adding tolerance slack. Charge any precision-increase reruns.

### Cancellation And Tightness

The self term in `BB^T y` and `q*y` is exactly the same mathematical quantity, but separately accumulated intervals forget that dependency. Subtraction remains valid with the endpoint rule above, although the enclosure can be wider than the actual residual. Higher precision, correlation-aware evaluation or a positively accumulated self-excluding representation can tighten it; these are separately priced choices, not requirements for soundness.

This native case is less pathological than arbitrary weighted factors. For nonnegative x,

```text
sum_i q_i*y_i <= sum_active_i x_i/(smin-1)
sum_i (B B^T y)_i = sum_active_i x_i + sum_i q_i*y_i.
```

For signed x the corresponding absolute-value bound uses `||x_active||_1`. Thus discarded diagonal mass cannot be arbitrarily large relative to candidate mass at fixed smin. This does not remove accumulation error from many memberships, factor reuse, large row fan-out, or division by `1-a`. Close-to-one damping still amplifies the certification burden.

For an exact reconstructed candidate, with `r_h=g-C h`, direct substitution also gives

```text
b + a P X(h) - X(h) = a B r_h           on active vertices
isolate residual = 0
||original residual||_1 <= a*sum_f s_f*abs(r_h[f]).
```

This is a useful cheap stopping screen, not a replacement for the planned final-file certificate. It requires the same h in both residual and reconstruction and is not automatically true of their independently rounded implementations.

## RAM And Work

Distinguish resident bytes, evolving scalar payload, persistent files, temporary disk, and arithmetic. No result here measures any of them.

| Item | Honest accounting |
| --- | --- |
| Resident fused ordinary power | `8*N_active + 16*F` bytes of f64 iterate payload, plus metadata/buffers. Isolates need no mutable rank entries. |
| Reduced power | `16*F` bytes of f64 iterate payload. Against that resident baseline, the selected-payload ratio is `1+N_active/(2F)`, not a universal `N/F` or application-RAM ratio. |
| Factor Jacobi / CG | Price actual simultaneous arrays, diagonal/RHS storage, preconditioning, dot products and residual replacements. CG is not a two-vector solver merely because its state is factor-sized. |
| Final certificate | At least two Decimal endpoints per factor, with coefficient storage, object/container overhead and temporaries. Not `16*F` bytes. It can dominate peak RAM even after float iterates are released. |
| Dense C | Quadratic storage; rowwise assembly performs up to `sum_i q_i^2` pair contributions, not just L incidence visits. Dense factorization is cubic in F. |
| Row fusion | Dotting a row precedes scattering its scalar back to its memberships. This uses two traversals of its membership list. If a row exceeds the buffer, reread/spool/duplicate-layout costs must be counted. One logical pass is not always one physical read of each incidence byte. |
| Metadata | Exact d and q, rational personalization, IDs, dictionaries, offsets, group sizes and sorting buffers are not free. O(F) solver state does not imply O(F) total RAM when these are resident. |
| Input preparation | Deduplication within groups, singleton detection, row ordering, joins and source validation require time/scratch. Avoiding expanded cliques does not eliminate these costs. |
| Publication | Reconstruct/write N scores and IDs, then two output-plus-row scans. Failed certificates and retries add complete work; no hidden reuse of a solver's h in place of a file gather. |

An important incumbent is **externalized ordinary power**: read the old entity scores sequentially with the row stream, write new scores sequentially, and retain only old/new F gathers in RAM. It has O(F) resident iterative state too, while paying N-score read/write traffic each update and live-disk/crash-consistency costs. An appropriately managed in-place file is another possible tradeoff. Accordingly, the factor-only method avoids an evolving entity file; it does not prove that ordinary PageRank intrinsically requires N resident scores.

A useful result must report retained F versus active N, not merely the large number of implied clique edges. Native factor counts can exceed entity counts. Report actual read/write bytes, row rereads, output/certification time, preparation and cold/warm behavior. Include process/runtime and cache policy in peak-memory reporting. Parallel local gather arrays multiply factor state and change summation order.

## Primary Precedents

These are bounded inspection results, not a comprehensive priority search. Source-specific statements below are deliberately narrower than the algebraic deductions above.

| Primary work and precise inspected location | What it establishes; limit of the comparison |
| --- | --- |
| **Liu et al., High-Order Line Graphs of Non-Uniform Hypergraphs, IPDPS 2022**, [author-hosted PDF, Section III-H/III-I, printed p. 789, PDF p. 6](https://sinanaksoy.com/publication/liu-2022-high/liu-2022-high.pdf#page=6) | Explicitly gives weighted clique adjacency `HH^T-D_V`. Section III-I and Table II compute PageRank on clique and higher-threshold projections. The thresholded graphs are not automatically the same multiplicity-weighted operator as this review. This establishes clique-expansion/PageRank precedent, not the exact factor-only solver. |
| **Aleja et al., Matrix-based pagerank control in hypergraphs for semantic text summaries**, [publisher article](https://www.nature.com/articles/s41598-025-32380-5), Definition 2.1 and Theorems 3.1-3.3 | Definition 2.1 is exactly the off-diagonal shared-group count. Theorem 3.1 transfers PageRank between row-stochastic rectangular products AB and BA; the following reconstruction and Theorems 3.2-3.3 describe recovery/evolution. Published online 15 December 2025, volume 16 (2026), article 2481. Its factors are not automatically this binary B, and its stochastic-product theorem is not itself the Q-absorbed SPD solve. Nevertheless, factor-space PageRank and reconstruction cannot be claimed as new in general. |
| **Shen et al., Off-diagonal low-rank preconditioner for difficult PageRank problems, 2019**, [repository publisher PDF, Section 5.1, Lemma/Theorem 5.1, equation (14), printed pp. 460-461](https://pure.rug.nl/ws/portalfiles/portal/103307690/1_s2.0_S0377042718304357_main.pdf#page=6) | Explicit Woodbury/capacitance formulation `D_lit+F_lit H_lit`, with capacitance `I+H_lit D_lit^-1 F_lit`. Substituting `D_lit=K`, `F_lit=-aB`, `H_lit=B^T` gives this C exactly. That substitution is our specialization, not a claim that their implementation is a native-incidence streamed solver. |
| **Chitra and Raphael, Random Walks on Hypergraphs with Edge-Dependent Vertex Weights, ICML 2019**, [proceedings PDF, Section 3, equation (1), Theorem 3.1, PDF pp. 3-4](https://proceedings.mlr.press/v97/chitra19a/chitra19a.pdf#page=3) | Incidence-like factored transitions and clique-walk equivalence are explicit. Their displayed walk allows self-loops and uses hyperedge normalization; it must not be substituted as an exact oracle for the present loop-free unnormalized projection. The paper itself flags the non-lazy distinction. |
| **Hestenes and Stiefel, Methods of Conjugate Gradients for Solving Linear Systems, 1952**, [NIST primary paper, Sections 2-3, printed p. 410](https://math.nist.gov/mcsd/Reports/2002/hestenes-steifel-52.pdf#page=2) | SPD CG, initial guesses, residuals and exact-arithmetic finite termination are established. Applying that method to the proved-SPD C is ordinary specialization; the source does not concern hypergraph PageRank. Finite-precision CG needs independent residual validation. |
| **Gleich, PageRank Beyond the Web**, [Section 2, Aside 2.3](https://arxiv.org/pdf/1407.5107#page=3) | Supplies the original-space residual-to-L1-error bound reused here. Its use does not validate a compiler or a rounded residual evaluator. |

**Inspection/access ledger:** Read extracted primary PDF text for Liu, Shen, Chitra and Gleich. For Aleja, inspected the publisher's definition/date text, institution-hosted published PDF opening pages, and publisher-indexed full theorem statements; follow-up opens intermittently returned internal errors and PMC switched to a CAPTCHA. The supplement proofs were not inspected. For Hestenes-Stiefel, inspected NIST-indexed original Section 2/3 text; PDF text extraction/screenshot retrieval failed, so no visual equation audit is claimed. ODLR screenshot retrieval also failed, while its extracted equation (14) was readable. Python API claims were checked against the official Decimal documentation.

No inspected source was established here to contain the exact complete combination of binary native rows, absorbed diagonal Q, F-state matrix-free CG, bounded row fusion and final-file interval certificate. **That limited observation is not novelty evidence:** the exact algebra already follows from inspected capacitance precedent. The prior-art input's WDBMC Section 3 access gap remains unresolved; this sidecar did not obtain or re-audit that manuscript. No paywall/CAPTCHA bypass, credentials, author contact or manuscript request was attempted.

## Final Synthesis

The useful contribution to investigate is a **certified native-incidence execution result**, potentially a source-admission/solver-selection policy with explicit RAM, disk and pass budgets. The elementary size bound can support that policy. Its usefulness would come from validated end-to-end behavior and good decisions, not invention of elimination, CG, clique PageRank, or residual error bounds.

The finite decision gates for the lead are:

1. Admit the exact source semantics, preserve the full universe, and compute all degree data exactly.
2. Compare fused ordinary power, reduced power, ordinary factor Jacobi and admitted CG/dense controls with identical final-file certification. Include externalized ordinary power when claiming a RAM-capability advantage.
3. Separate avoided clique materialization from eliminated entity state; give every incidence-aware method the same prepared source.
4. Count initialization, oversized-row handling, serialization, two certificate scans, Decimal peak memory and any retries. Report losses and cases where F is too large.
5. Classify a successful natural-source experiment as evidence for A01's usefulness, not proof of a globally novel algorithm or the general defect/release planner.

**Remaining uncertainty:** practical performance, numerical implementation correctness, complete primary-art coverage and any genuine distinct planner contribution. None is resolved by the valid algebra alone. This bounded review is complete; no further work was launched.

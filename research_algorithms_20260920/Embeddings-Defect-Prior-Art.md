# Embedding Defects And Lattice Replay: Independent Review

Date: 2026-09-20. Bounded primary-source and correctness/resource review only. No implementation of the lead's algorithm, old-suite rerun, or change to another manuscript.

## Findings First

**The numerical core survives, but its elementary stability argument is established territory.** Exact integer replay is exact execution of a *rounded surrogate*, with a certified error against the original powers; it is not exact evaluation of those powers. The useful local candidate is the combination of bounded-width feature histories, no resident node-vector/norm planes, and certified whole-row publication. Novelty against a competent factor-aware replay executor remains unresolved.

Independent inspection of [the lattice manuscript](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Embeddings-Lattice-History-Replay.md) found:

1. **Sound under its stated assumptions:** self-excluded integer recurrence; equivalence to full-state nearest-even propagation; range preservation; `t*Delta/2` coordinate error; positive-denominator `beta`; signed dyadic interval accumulation and final representability check. Proof checks appear below.
2. **Small width qualification:** at lines 73-81, the numerator bound covers numerator *values*, but cannot also reserve every divisor operand when `M=0`. For example, a two-node feature of weight `10^100` has a zero numerator and a 333-bit divisor, while the displayed numerator reservation is three bits. Admit metadata/division operands separately, use `max(1,M)` inside the product for that workspace bound, or take an independently validated all-zero fast path. This does not invalidate the recurrence/error theorem.
3. **Precision-recipe qualification:** lines 87-94 give enough accuracy for an ideal norm-based perturbation estimate, not guaranteed completion of the coarser `floor(sqrt(S))`/`ceil(sqrt(d))` publication check. That check remains safe because it can reject. Do not turn the recipe into a completion theorem without budgeting the implemented bound.
4. **Resource result still open:** bit counts and tiny fixture success do not establish a 4 GB physical execution or a full 50 GB lifecycle. Packed representation, metadata builder, arithmetic workspace, output staging, and refresh coexistence remain to be priced and measured. The manuscript already acknowledges most of this boundary.

These line references identify the inspected draft, not immutable anchors. The corrected metadata helper returns only `q, degree`; dense construction is inside `validate_lattice_replay_fixture`, outside the history builder. The probe still intentionally keeps dense oracle and initialization tables. It is not a production-memory demonstration.

## Inspected Primary Art

Six focused sources, not an exhaustive priority search. Each statement below is based on inspected full-text sections, not a search-result abstract. Subsequent algebra/counterexamples are this review's deductions.

| Source And Inspected Location | Closest Result And Boundary |
|---|---|
| Nedic, Olshevsky, Ozdaglar, Tsitsiklis, *On Distributed Averaging Algorithms and Quantization Effects* (2009), Section V, update (14), Proposition 20 proof; Section VI(c). [Published author PDF](https://www.mit.edu/~jnt/Papers/J123-09-quant-averaging.pdf), [inspectable preprint math, Section 5.4](https://arxiv.org/html/0711.4179v1#S5.SS4). | Particularly close precedent: quantize each stochastic average on a fixed grid. The proof explicitly compares quantized/unquantized trajectories with a linear-in-time `t/Q` error. It uses round-down; consensus results assume doubly stochastic matrices, positive diagonals and connectivity. Section VI(c) explicitly leaves finite-precision computation of the average outside its model. Our loop-free, nearest-even, finite-horizon problem does not inherit its consensus theorem. Nevertheless, the linear accumulation principle is plainly not new. |
| Higham and Knight, *Matrix Powers in Finite Precision Arithmetic* (1995), Sections 2-3, equations (3.1)-(3.2). [Full primary PDF](https://eprints.maths.manchester.ac.uk/345/1/0616025.pdf). | Models repeated floating-point products as products of perturbed matrices and analyzes norm growth/convergence. Section 3 notes `abs(A)=A` for Markov transition matrices. It expressly distinguishes repeated multiplication from binary powering because their finite-precision semantics differ. This is direct matrix-power precedent, but not a fixed-grid feature-history algorithm or a normalized-row certificate. |
| Botchev, Grimm, Hochbruck, *Residual, Restarting, and Richardson Iteration for the Matrix Exponential* (2013), Section 4, (4.1)-(4.8), Lemma 4.1; Section 2 residual representation. [Primary PDF](https://na.math.kit.edu/download/papers/BotchevGrimmHochbruck-2013.pdf). | Treats residual as forcing/backward defect, propagates it by variation of constants, and bounds forward error through the propagator norm. Its Krylov residual has a compact representation. This is the nearest inspected defect-certification principle. The theorem concerns continuous-time exponential action, not discrete powers or row normalization. Compact residual representation also does not by itself eliminate the vectors needed to obtain it. |
| Tobias Jawecki, *A Study of Defect-Based Error Estimates for the Krylov Approximation of Phi-Functions*, inspected PDF dated November 9, 2021, Theorem 1, equation (3.2a), Remark 3. [Primary PDF](https://arxiv.org/pdf/2001.11922). | Error representation includes an additional Arnoldi-decomposition perturbation term. Remark 3 explicitly excludes rounding in some reduced-function and reconstruction substeps. Useful warning: a theorem accounting for one source of roundoff is not an end-to-end certificate. Its Euclidean dissipativity hypotheses cannot be inferred from row stochasticity. |
| Ogita, Rump, Oishi, *Accurate Sum and Dot Product* (2005), Section 2; Algorithm 5.8 and Corollary 5.9. [Author-hosted published PDF](https://ogilab.w.waseda.jp/ogita/math/doc/2005_OgRuOi.pdf). | `Dot2Err` computes an enclosing error bound using working-precision operations, including an underflow allowance, under its arithmetic assumptions and successful completion. No overflow is assumed. This supplies a serious validated-reduction comparator, not merely naive summation. Compensated summation without its bound is not automatically a certificate. |
| Xiaobo Liu, *Mixed-Precision Paterson-Stockmeyer Method for Evaluating Polynomials of Matrices*, Theorem 2.1, Lemma 2.2 and Theorem 2.3. [Primary PDF](https://arxiv.org/pdf/2312.17396). | Analyzes forward errors with different precisions across polynomial evaluation; its useful precision schedule depends on coefficient/power magnitudes. Required matrix powers are explicitly formed. This establishes relevant mixed-precision polynomial art, not a no-node-state graph action, nonlinear rounded recurrence, or normalized-row guarantee. |

The closest match to the new `t*Delta/2` claim is **quantized averaging**, not merely a matrix-exponential analogy. None of these inspected sections establishes the complete proposed storage/publication schedule. That limited observation is not evidence of publication novelty.

## Defect Route: What Actually Certifies

Use the operator from [Replay-Embeddings.md](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Replay-Embeddings.md): binary incidence `B`, positive integer weights, active feature sizes at least two, `P=D+UV^T`, `V=B`, and on nonisolates `D_ii=-q_i/s_i`, `U_if=B_if*w_f/s_i`; isolate rows are zero. Thus `P>=0`, row sums are at most one, and on nonisolates `sum_f U_if=q_i/s_i<=1`.

For an ideal exact replay of stored histories define

```text
delta_t = Hhat_t - V^T Ehat_t
rho_t   = Ehat_(t+1) - P Ehat_t = U delta_t.
```

For actually rounded reconstruction there is an additional term:

```text
omega_t = Ehat_(t+1) - D Ehat_t - U Hhat_t
rho_t   = U delta_t + omega_t.
```

Coefficient approximation, reconstruction arithmetic and initialization errors cannot disappear into notation. With `F_t=Ehat_t-E_t`, direct substitution gives the discrete forced-recurrence identity

```text
F_t = P^t F_0 + sum_(j=0)^(t-1) P^(t-1-j) rho_j.
```

Both the entrywise maximum norm and `||X||row=max_i ||X[i,:]||_2` are nonexpansive under left multiplication by this `P`: each output row is a nonnegative combination of input rows with total weight at most one. Therefore

```text
||F_t|| <= ||F_0|| + sum_(j<t) ||rho_j||,
||U delta_t||row <= ||delta_t||row.
```

This is the discrete counterpart of the established forced-error principle in the sources above, not a new residual theorem. It needs every relevant prior residual, not only the last one. It is an additive forcing interpretation; it does not prove that one small, fixed, incidence-preserving stochastic perturbation of `P` explains the entire history.

The lattice route makes `H_t=B^T e_t` exact. Its history-reduction defect is zero; the nonzero defect is the explicitly bounded final division rounding. Thus the two routes are related but should not be conflated: the lattice theorem avoids having to certify an approximate feature reduction at every step.

## Lattice Correctness Review

### Recurrence And Range

Write `X_t` for the manuscript's integer `e_t`, `R=Delta*X_0`, and `M=max|X_0|`. Exact binary membership gives, coordinatewise,

```text
sum_f B_if*w_f*H_tf - q_i X_ti
  = sum_(j!=i) W_ij X_tj.
```

Consequently the stated history reconstruction is exactly the full-state integer algorithm `X_(t+1)=RNE(P X_t)`. Freeze each complete history layer before consuming it; partial scatters are not legitimate histories. Duplicate memberships or mixing metadata/history generations breaks the identity.

Nonisolate rows average values in `[-M,M]`. Because both endpoints are integers, nearest-even rounding stays inside that interval. Isolates map to zero. This proves the range claim independently of cancellation in the signed factorization.

For `Ehat_t=Delta*X_t`, each local division contributes at most `Delta/2` absolute error. Applying the preceding nonexpansive recurrence yields

```text
||Ehat_t-P^t R||max <= t*Delta/2,
||Ehat_t[i,:]-E_t[i,:]||_2 <= sqrt(d)*t*Delta/2.
```

The bound is correct for exact-grid initialization and exact integer intermediates. With initial error `eta_0` in the maximum norm, replace the first bound by `eta_0+t*Delta/2`. Deterministic ties are not independent noise: no square-root-in-time improvement follows. Row-stochasticity is not strict contraction, and no consensus or asymptotic convergence claim is needed.

### Widths And Nonlinearity

History partial sums satisfy `|H_tf|<=t_f*M<=n*M`. Absolute partial weighted gathers satisfy `(s_i+q_i)M`; inclusion of the self term gives `(s_i+2q_i)M<=3s_iM`. The displayed signed widths are conservative for those **values**, including unfavorable summation order. Final numerator size `s_i*M` alone would underreserve intermediate cancellation.

For `M>=1`, the conservative numerator reservation also dominates the divisor and doubled remainder. For `M=0`, handle the operand-width exception in Finding 2. Empty incidence/all-isolate graphs also need a defined `s_max=0` convention. Metadata widths and integer multiply/divide implementation scratch are separate from an output-value bit bound. Optional dyadic initialization can make `M` large; being independent of the degree LCM does not mean being independent of requested precision or input exponent range.

The `O(L^2)` replay cost is necessary for the described schedule, not a proved lower bound for every conceivable method. The old grouped *linear* recurrence cannot be substituted. A stronger counterexample than `round(a)+round(b)!=round(a+b)` is a legal one-feature K3:

```text
X_0=(1,1,-2), H_0=0, D=-I/2.
P X_0=(-1/2,-1/2,1).
RNE(P X_0)=(0,0,1), so H_1=1.
```

The zero initialization also has `H_0=0` but gives `H_1=0`. Even with one diagonal group and one feature, the aggregate alone cannot determine the next rounded aggregate. A histogram of row residues might restore more information, but its size/work would need a new paid analysis. No such closure is established here.

### Normalizer And Publication

Let `x=E_t[i,:]/Delta`, integer candidate `e=X_t[i,:]`, `S=sum e_j^2`, `h=floor(sqrt(S))`, `c=ceil(sqrt(d))`, and `r=t*c/2`. Then `||e-x||_2<=r` and `||x||_2>=h-r`. If `2h>tc`, the true row is nonzero. The triangle inequality and reverse triangle inequality give

```text
||normalize(e)-normalize(x)||_2
  <= 2||e-x||_2 / ||x||_2
  <= 2r/(h-r) = 2tc/(2h-tc).
```

Thus the manuscript's `beta` is valid, including its factor of two. It may exceed two and be needlessly conservative; that affects admission, not correctness. The sharper exact integer test `4S>t^2*d` also proves nonzeroness, but does not by itself supply the manuscript's dyadic publication bound. No replacement implementation is proposed here.

The dyadic enclosure is sound: `z=isqrt(S*2^(2p_s))` gives a lower square-root endpoint, `z+1` an upper endpoint. For nonzero integer `e` and nonnegative `p_s`, `z>0`. Positive coordinates use the larger denominator for the lower bound; negative coordinates reverse that choice. Outward integer division and upward rounding of `beta*2^p_o` preserve enclosure. Multiplication by a negative `alpha` must swap endpoints, as the probe does. Finite dyadic alphas permit a common power-of-two denominator rather than a growing product of unrelated denominators.

If the true coordinate is enclosed by `[lo,hi]`, checking both exact distances from the actual returned float proves its absolute-error contract. A rounded midpoint alone would not. The production boundary must explicitly reject nonfinite conversion/overflow, admit alpha exponent span and accumulator widths, and validate layer/alpha lengths; the tiny Python fixture is not that adapter. The executed probe covers binary64 conversion, not a binary32 encoder.

**Recipe qualification witness.** On K3 take two-dimensional initialization `((3,3),(2,2),(3,3))`, `Delta=1`, and only layer one weighted by one. The true contributing norm floor is `mu=5*sqrt(2)/2`, and `eta=sqrt(2)/2`, so `eta/mu=1/5`. Both advertised recipe inequalities hold at `tau=8/5`. Two rounded rows are `(2,2)`, however, giving `h=2,c=2,r=1,beta=2>tau`. The actual publication procedure rejects those rows. This is a conservative refusal, not an incorrect accepted answer; a claimed completion condition must use the actual bound and endpoint widths.

**Zero boundary remains.** A computed zero at positive depth does not prove a true zero. For the manuscript's same-real-input K3 `(0,1,1)`, the exact layer is `(1,1/2,1/2)` while the unit-grid layer is `(1,0,0)`. The true normalized entries are all one. Refinement can rescue this fixture; no uniform nonzero norm floor or finite retry count follows for arbitrary initialization. The earlier symmetric K4 exact-zero case still requires a structural zero certificate or an exact route, not inference from a small residual.

## Other Exact Falsifiers

- **Wrong norm:** for an undirected star with `m` leaves, let a scalar column be one at the center and zero at leaves. Its Euclidean/Frobenius norm changes from one to `sqrt(m)` after applying the row-normalized `P`; its maximum norm stays one. Reversible degree-weighted Euclidean bounds are possible, but recovering a bound at row `i` costs a factor involving `1/sqrt(pi_i)`; zero-degree rows need separate handling.
- **Discarded history:** on K2, take exact initial zero and candidate layers `Ehat_1=(1,1)`, `Ehat_2=(1,1)`. The last residual is zero while the error remains one. Earlier forcing cannot be forgotten.
- **False computed defect:** the exact sum of `(2^53,1,-2^53)` is one, while the displayed sequential binary64 reduction is zero. Repeating that same reduction against stored zero reports zero discrepancy. As a K3 feature reduction it misses a true `U*delta` of `-1/2` per row. A checked reduction needs an enclosure or exact accumulator, not agreement of two identically rounded calculations.
- **Absolute-factor pessimism:** on the nonisolate block with size-two features, `D=-I` and `UV^T=I+P`. The naive absolute-factor operator is `2I+P`, of infinity norm three, whereas `P` has norm one. This refutes that particular error enclosure, not all interval, correlated, residual, or dependency-aware methods.
- **Normalization discontinuity:** `x=0`, `ehat=epsilon*v`, `||v||_2=1`, has arbitrarily small raw error but normalized error one. For small positive rows opposing directions can give error two. No absolute raw bound alone solves the zero decision.
- **Publication floor:** a legal output coordinate `1/3` cannot satisfy absolute tolerance `10^-20` in binary64 (or binary32). For example an exact normalized `(1,2,2)` row has that coordinate. The nearest binary64 error is `1/54043195528445952`. More replay precision cannot fix the output format.

## Strong Same-Factor Controls

| Comparator Given The Same Incidence, Metadata And Numeric Contract | Required Comparison |
|---|---|
| Full-state exact-integer fixed-point propagation, with the same grid and tie rule. | Identical integer trajectories and raw certificate. It needs no expanded graph: form feature sums from the current node plane, freeze them, then update each node using its own old row. One raw node plane can be overwritten after that freeze; do not force an unnecessary second raw plane on this competitor. Multi-layer normalized accumulation needs its own paid storage/replay strategy. |
| The same full-state competitor with the node plane on disk. | Potentially admissible under 50 GB even when RAM fails. It can trade sequential `n*d` traffic for avoiding quadratic replay. Compare total graph scans, vector reads/writes, normalized-output state and elapsed time, not merely resident bytes. |
| Generic factor-aware replayer supplied the same frozen integer histories. | Can reproduce exactly the proposed row recurrence, bit widths and certificate without a resident node plane. Against this control there is **no established asymptotic or numerical separation**. Calling the primitive a graph embedding does not establish one. |
| Factor-aware floating/reduced-precision histories with validated reductions and the defect identity above. | Give it the same factorization and replay access. Charge feature verifier accumulators and replay passes; do not require an `n*d` residual plane. It may obtain tighter data-dependent errors, but coefficient/reconstruction error and output normalization remain its obligations. |
| Exact grouped recurrence or component-local rational/clique solver. | Different arithmetic path to the same real target. The global-Q bit reservation is not the strongest exact baseline. For a single uniform feature, `P=(J-I)/(n-1)`, so `P^t x=mean(x)*1+(-1/(n-1))^t*(x-mean(x)*1)` supplies a strong matched specialized control. Multiple disconnected cliques can use local degrees rather than a global LCM. |

The last formula is direct eigen-decomposition of this operator, not a novelty claim. Uniform-feature benchmarks must include it, while using the same initialization, layer weights, output tolerance and zero treatment. A constant first coordinate legitimately improves conditioning but cannot silently stand in for the default signed initialization distribution.

## Resource Review

Let `m` be memberships, `w_H,w_N` admitted history/numerator bits, and `u_H=ceil(w_H/8)`, `u_N=ceil(w_N/8)` for an explicitly packed layout. Conservative admission reservations for this schedule require

```text
worker_budget >= (L+1)*F*d*u_H + L*d*u_N
       + resident_metadata + integer_arithmetic_workspace
       + norm/output_accumulators + bounded_IO + runtime.

retention_budget >= prepared_incidence_and_metadata + persisted_histories
                 + staged/final_output + pinned_old_generations
                 + other_portfolio_retention.

build_replay_integer_work = O((m+n)*d*L^2), plus O(L*F*d) clearing;
final_replay_integer_work = O((m+n)*d*L), plus normalization/conversion.
```

These are operation counts, not constant-time big-integer bounds. Multiply/divide/isqrt costs depend on the admitted widths. For `M>=1`, `S<=d*M^2` needs about `2 log2(M)+log2(d)` bits; the scaled square-root operand adds `2p_s`. Coordinate division operands add `p_s+p_o`; output sums add alpha exponent span and magnitude, layer accumulation, and sign. Retaining generic rational endpoint denominators instead would lose the claimed dyadic workspace advantage.

With resident history, the row-gather slab permits `O(L)` graph-volume passes, not necessarily `O(L^2)` physical graph reads: it gathers all prior depths during one row scan. Arithmetic and history-memory traffic still grow quadratically. A simple two-cursor realization reads membership data roughly twice per history pass and once at publication, plus metadata/initialization streams. An alternative cursor/cache policy must be measured, not assumed free. Preparation may require external sorting/counting and input validation; no larger builder is exempt.

**Illustrative admission scale, not a benchmark:** `n=20 million,F=100,000,d=64,L=8,b=24,M=2^24` gives a conservative 51-bit history width. Using eight-byte cells, history plus a separate current accumulator is 460.8 MB before other memory. One four-byte raw node plane is 5.12 GB, so its external-memory competitor is relevant. Complete float32 output is 5.12 GB plus IDs and framing; it remains a substantial charged artifact. No physical-cap conclusion follows without the omitted terms. At `n=100 million,d=128`, even values-only float32 output is 51.2 GB: retaining it violates the entire 50 GB allowance before graph/history data.

Use decimal 4 GB physical and a provisional, unmeasured 3 GB worker/1 GB host split from the research context. Bound buffers and backpressure through the client. Stage output until every row certifies; rejected attempts and retries are real I/O. Refresh must pin one consistent tuple of incidence, weights, degrees, IDs/seed, grid, history and output contract. If old/new generations cannot coexist within retention, use a documented rebuild/offline contract rather than mixed-generation reads.

## Evidence And Revised Conclusions

**Lead evidence, not independently rerun here:** the inspected receipt reports 72 fixtures, 6,723 raw-coordinate checks, 448 accepted rows and 72 complete jobs; the 106-node, eight-feature, depth-24 prime case checks 7,950 coordinates and completes all rows. Its stated 652-bit global-Q reservation versus 33-bit lattice reservation is correctly limited to that global baseline. The receipt records the metadata/oracle separation correction and the same real `(0,1,1)` fixture rejecting at `b=0` and completing at `b=18`. No physical RSS/retention test is claimed.

**Independent new evidence:** five small scalar assertions executed via `python3 -c`, exit 0. They checked the K3 loss of zero aggregate, the false binary64 reduction, the `M=0` three-bit/333-bit workspace mismatch, the exact `beta=2>tau=8/5` recipe witness, and the exact binary64 error for `1/3`. These did not import, run or reimplement the lead replay algorithm and did not rerun any old suite. No throughput or randomized success rate is inferred from them.

The audit changed the strongest claims as follows:

- From residual folklore as the nearest comparison to an explicit fixed-grid averaging predecessor with a linear finite-horizon error argument.
- From small final numerators implying small total workspace to separate operand, arithmetic, normalization and publication reservations, including the all-zero edge case.
- From a norm-floor precision recipe implying concrete completion to a safe actual-output test with additional conservative refusals.
- From few diagonal values suggesting the old fast closure to a legal single-group counterexample; retain quadratic replay until a different bounded-state proof exists.
- From comparison with a dense or global-Q-only baseline to incidence-native, external-plane, generic replay and component-local exact controls.

**Surviving contribution candidate:** a resource-admitted composition that exchanges degree-LCM-dependent exact-history growth for fixed-grid histories, while preserving a rigorous error contract for the original normalized graph powers and refusing undecidable output. **Unresolved hard gap:** demonstrate useful completion and an end-to-end advantage over the same-factor controls, including norm-distribution, output-format and lifecycle costs. Neither a new general numerical theorem nor a differentiated algorithm against the generic same-history replayer is established by the present evidence.

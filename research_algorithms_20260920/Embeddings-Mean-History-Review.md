# Quantized Mean Histories: Independent Review

Date: 2026-09-20. Scope: challenge the supplied candidate, not implement its replay/builder or rerun a lead suite. Only this new file is owned. Arithmetic below assumes the exact operator and output contract in [the lattice manuscript](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Embeddings-Lattice-History-Replay.md). The concluding manuscript check also covers [the mean-history draft](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Embeddings-Mean-History-Replay.md), including its observed-defect gate.

## Bounded Conclusion

The proposed `3t*Delta/2` raw-error bound and `6tc/(2h-3tc)` normalizer are **valid** with exact temporary sums, nearest-even rational rounding, the stated clip, and consistent metadata. Retained integer means remove feature cardinality from each retained scalar's range; temporary exact sums and reconstruction arithmetic do not lose that dependence.

Three useful refinements survive independent challenge:

1. `3/2` is a tight universal **one-step** error constant, even when clipping does nothing.
2. With `p>=0` integer fractional mean bits, the proposed coefficient is safe but generally loose. Integer cardinality gives the sharper universal coefficient `lambda_p=1/2+1/(2^(p+1)-1)`. A feature-specific bound can improve it further.
3. For `p>=1`, clipping is provably a no-op under this contract. For `p=0`, it is needed to establish the advertised invariant range in general.

The strongest matched comparator is a generic factor-aware quantizer of sums with scale `t_f/2^p`: it stores exactly the same integers and has exactly the same errors. There is no established algorithmic separation from it. The contribution candidate is a certified storage/precision tradeoff inside graph replay, not a new quantization primitive. Packed lifecycle advantage remains unproved.

## Contract And Basic Proof

Write `X_t` for integer node coordinates, `Ehat_t=Delta*X_t`, `R=Delta*X_0`, and `M=max|X_0|`. Binary memberships are unique, feature weights are positive integers, active feature sizes `t_f>=2`, and

```text
H_tf = sum_i B_if X_ti                  (temporary exact integer sum)
q_i  = sum_f B_if w_f
s_i  = sum_f B_if w_f (t_f-1)
P_ij = W_ij/s_i for i!=j, W_ii=0; isolate rows are zero.
```

For integer `p>=0`, the unambiguous fractional-mean contract is

```text
A_tf = RNE(2^p H_tf / t_f)              (retained integer)
a_tf = A_tf / 2^p
u_i  = (sum_f B_if w_f t_f A_tf - 2^p q_i X_ti) / (2^p s_i)
X_(t+1),i = clip[-M,M](RNE(u_i)).
```

This is a specification, not an implementation. An isolate is assigned zero after layer zero. The factor `2^p` on the self term is essential. Round each completed feature mean once and each completed node numerator once; rounding intermediate feature contributions is a different algorithm. A complete frozen history must be derived from the same candidate layer that replay reconstructs.

Let `epsilon_f=a_tf-H_tf/t_f`. Exact arithmetic gives

```text
u_i = (P X_t)_i + zeta_i,
zeta_i = sum_f B_if w_f t_f epsilon_f / s_i.
```

Nearest-even mean rounding gives `|epsilon_f|<=2^(-p-1)`, hence

```text
|zeta_i| <= (1+q_i/s_i)/2^(p+1),
|RNE(u_i)-(P X_t)_i| <= 1/2+(1+q_i/s_i)/2^(p+1).
```

Clipping to an interval containing `(P X_t)_i` cannot increase distance from that point. Induction establishes `|X_t|<=M`. The original nonnegative substochastic `P` is nonexpansive in the entrywise maximum norm, so with a uniform local bound `lambda`,

```text
||Delta*X_t - P^t R||max <= t*lambda*Delta.
```

For `p=0`, `q_i<=s_i` gives `lambda=3/2`. This validates the user's inference. Non-grid initialization adds its initial maximum error; it cannot silently use this zero-initial-error formula. The error may also be capped by `2M*Delta` since both trajectories lie in the invariant box. No claim is made that the linear-in-depth bound is simultaneously attained at every depth.

## Sharper Integer-Denominator Bound

This is a deduction from the specified rational grid, not a claimed new published quantization theorem. Define, for each feature,

```text
g_f = gcd(t_f,2^p), r_f = t_f/g_f,
epsilon_max(f,p) = floor(r_f/2)/(r_f*2^p).
gamma_f(p) = t_f*epsilon_max(f,p)/(t_f-1).
```

The fractional part of `2^p H_tf/t_f` is a multiple of `1/r_f`. Its distance to an integer is at most `floor(r_f/2)/r_f`, proving the bound including negative sums and even ties. Thus a sharper graph-dependent local coefficient is

```text
lambda_i(p) = 1/2 + sum_f B_if w_f t_f epsilon_max(f,p) / s_i.
```

The mean-error term is a convex combination of the `gamma_f(p)`, with weights `B_if*w_f*(t_f-1)/s_i`. No node-sized error table is necessary to retain the scalar maximum of `lambda_i`, although computing it requires a paid metadata/membership pass.

If `r_f` is odd, `gamma_f(p)=(t_f-g_f)/(2^(p+1)*(t_f-1))<=2^(-p-1)`. If `r_f` is even, `g_f=2^p`, `t_f>=2^(p+1)`, and

```text
gamma_f(p) = t_f/(2^(p+1)*(t_f-1)) <= 1/(2^(p+1)-1).
```

Consequently, for every admitted graph,

```text
lambda_p = 1/2 + 1/(2^(p+1)-1)
         = 3/2, 5/6, 9/14, ... for p=0,1,2,... .
```

This is stronger than the cardinality-oblivious `1/2+2^-p` for `p>=1`. If all `t_f` divide `2^p`, the mean sums are represented exactly and the mean-error term vanishes altogether. For `p=0` and exclusively odd cardinalities, the coefficient is at most one rather than `3/2`.

**Clipping consequence.** For `p>=1`, `|zeta_i|<=1/(2^(p+1)-1)<=1/3<1/2`. Since `(P X_t)_i` is in `[-M,M]` and the endpoints are integers, nearest-even rounding of `u_i` cannot cross either endpoint. Therefore clip changes nothing. Keeping it is harmless. This does not extend to stochastic rounding, rounded metadata, noninteger node state, or arbitrary feature scales.

## Tightness And Failure Witnesses

**Necessary clip at zero extra bits.** One K3 feature, `M=5`, `X_0=(-5,5,5)`: exact mean `5/3` rounds to two. The first pre-round update is `(3*2-(-5))/2=11/2`, which rounds to six. The true neighbor average is five. Clipping restores five and prevents a stored row exceeding the range used for subsequent mean-width bounds.

**Tight one-step errors without clipping.** Construct two size-`t_f` features sharing only the tested node, whose value is zero. Give the other members sums `H_1,H_2`; one member per feature can carry that sum and the remainder can be zero. Set `M=max(|H_1|,|H_2|)`. The following legal examples attain the sharper coefficient:

| p | Both Feature Sizes | H_1,H_2 | w_1,w_2 | Exact `(P X)_i` | Reconstructed `u_i` | Rounded Result | Error |
|---|---|---|---|---|---|---|---|
| 0 | 2 | -1,3 | 5,3 | 1/2 | 3/2 | 2 | 3/2 |
| 1 | 4 | 3,7 | 7,1 | 7/6 | 3/2 | 2 | 5/6 |
| 2 | 8 | 7,11 | 3,5 | 19/14 | 3/2 | 2 | 9/14 |

More generally let `t_f=2^(p+1)`, `m=t_f-1`, and choose the two consecutive integers congruent to three modulo four that bracket `3m/2-1`. Integer weights summing to eight realize that half-integer weighted average. Both rounded feature sums exceed their exact sums by one; `u_i=3/2` and the error is `1/2+1/m`. This establishes universal one-step sharpness, not just loose independent maxima of incompatible rounding events.

**Zero/sign failure despite valid clipping.** K2 with `M=1`, `X_0=(1,0)` and `p=0` has mean `1/2`, rounded to zero. The new state is `(-1,0)` although the true next state is `(0,1)`. The range is respected, but positivity and the sum are not preserved. Treating the first candidate row as a valid unit-normalized approximation would be wrong: the true first row is exactly zero. At `p=1` the mean is exact, illustrating that extra mean precision can fix this particular failure, not every normalized-output obstruction.

**No fixed-state error feedback for free.** Adding a residual from past mean rounding would change the specified trajectories. Exact residuals have denominator/cardinality state to price; stochastic rounding needs reproducible random decisions across replays and a changed guarantee. Neither permits retaining the present certificate unchanged.

## Normalized Publication

For `p=0`, one row has Euclidean error at most `3t*sqrt(d)/2` in lattice units. Set `c=ceil(sqrt(d))`, `h=floor(sqrt(sum_j X_tij^2))`, and `r=3tc/2`. If `2h>3tc`, the true row is nonzero and the standard normalization perturbation inequality gives

```text
||normalize(X_ti)-normalize(E_ti)||_2
  <= 2r/(h-r) = 6tc/(2h-3tc).
```

The supplied `beta` is correct. A nonpositive denominator means refusal, not a valid zero result. With an admitted sharper coefficient `lambda`, use `r=t*c*lambda` and the same `2r/(h-r)` argument. The coefficient must bound all prior forcing relevant to this row; replacing a global coefficient by only the current row's `lambda_i` is unsafe because errors arrive from neighbors.

Output normalization is not inside propagation. Multiply layer-error bounds by `|alpha_t|`, round margins outward onto the output dyadic lattice, and include normalization and final finite-format conversion error. Alpha cancellations cannot cancel worst-case error allowances. Near-zero rows, exact-zero decisions, representability floors, and complete-job publication retain the restrictions in [the earlier independent review](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Embeddings-Defect-Prior-Art.md). Nothing here proves a universal successful finite-precision completion route.

## Targeted Primary Comparison

Four inspected primary sources, limited to aggregation/history quantization and the directly relevant error distinction:

| Inspected Source | Match And Important Difference |
|---|---|
| Peng et al., *From Sancus to Sancus^q: Staleness and Quantization-Aware Full-Graph Decentralized Training in Graph Neural Networks* (2025), Section 3.6, equations (8)-(10), Theorem 4.8 and Corollary 4.9. [Primary full text](https://link.springer.com/article/10.1007/s00778-024-00897-2). | Direct precedent for quantizing and caching historical GNN embeddings with error control. It uses stochastic rounding with per-row scale/zero point and quantized broadcasts. Its histories are distributed node-embedding blocks, with training/staleness assumptions, not frozen feature means certifying every normalized power row. Its unbiased quantization theorem does not authorize a deterministic half-grid bound. |
| Zhu et al., *A2Q: Aggregation-Aware Quantization for Graph Neural Networks* (ICLR 2023), Section 3.1, equations (1)-(6). [Primary PDF](https://arxiv.org/pdf/2302.00193). | Uses learned step sizes/bitwidths, saturation, integer representations and a memory penalty; explicitly distinguishes aggregation and update. This rules out novelty claims for aggregation-aware precision selection or integer GNN intermediates generally. It is a trained node-feature scheme, not an exact-count replay theorem or universal normalized-coordinate certificate. |
| Liu et al., *EXACT: Scalable Graph Neural Networks Training via Extreme Activation Compression* (ICLR 2022), Figure 1, Section 3.1, Proposition 1 and Appendix E.1. [Inspected primary PDF](https://openreview.net/references/pdf?id=S3N-_EuFW5). | Retains compressed activations and reconstructs them for backward computation; accurate forward activations feed the next layer before compression. Its stochastic quantization analysis concerns unbiasedness/variance. Our compressed history participates in subsequent forward propagation, so errors accumulate differently. Do not confuse the acronym EXACT with exact real-arithmetic output. No empirical accuracy-drop claim is needed for this comparison. |
| Fey et al., *GNNAutoScale* (ICML 2021), equation (2), Section 2 storage discussion, Section 3 Lemma 1 and following paragraph. [Primary PDF](https://proceedings.mlr.press/v139/fey21a/fey21a.pdf). | Stores historical node embeddings outside the GPU and bounds approximation error. It explicitly distinguishes the neighborhood-size factor of sum aggregation from tighter mean/max aggregation bounds. Consequently, neither history reuse nor better scaling of mean-aggregation error is new in itself. External CPU/disk histories still count on a 4 GB physical machine; stale node histories are not the proposed frozen feature histories. |

These sources challenge broad novelty claims; none of the inspected passages supplies this exact feature-mean replay/publication contract. This bounded search does not establish priority for the composition or the denominator-aware refinement.

## Strongest Matched Controls

1. **Same factor, scaled-sum quantizer.** Let `T=diag(t_f)`. The candidate stores `A=RNE(2^p T^-1 B^T X)` and reconstructs the approximate sum `T A/2^p`. A generic quantizer of `H=B^T X` with feature scale `t_f/2^p` produces the same codewords. Given the same clip, replay order and certificate, this is a bit-for-bit comparator, not a weaker dense baseline. The change of variables supplies no algebraic novelty.
2. **Exact sums with feature-local widths.** Reserve `log(t_f M)` per feature, or the maximum active `t_f`, rather than forcing `log(nM)` uniformly. On size-two features with `p=1`, `A=RNE(2H/2)=H`: the mean code is literally the exact sum. No history-storage gain survives that match.
3. **Mixed exact/coarse features under one certificate.** Give a competitor exact storage on small or power-of-two cardinalities and coarse scaled storage elsewhere, using the corresponding local quantization-error allowances. Directory, scale and width metadata are paid. This is a stronger comparator than demanding one wasteful width everywhere; no implementation or optimal allocator is claimed here.
4. **Factor-aware node state on disk.** It can form the same means once per layer and update a raw node plane after means freeze, avoiding quadratic history replay at the cost of sequential node-vector traffic. Include its normalized-output accumulation state and output bytes. A permitted 50 GB retained disk makes this a genuine control when resident state fails.

Means introduce no new escape from nonlinear replay: retained histories are quantized, node rounding is nonlinear, and the zero-bit variant also clips. The old grouped linear closure is unavailable without a separate proof. Identical mean histories do not in general determine the next means without the row-specific states.

## Storage Tradeoff And Failure Region

Let `W(z)=2+bit_length(max(1,z))` denote the same conservative signed-value convention as the prior manuscript. Candidate value reservations are

```text
retained mean code:      W(2^p M)
temporary exact sum f:   W(t_f M)
mean-rounding numerator: W(2^p t_f M)
node gathered numerator: W(3 s_max 2^p M).
```

The last bound follows from `|A_tf|<=2^p M` and `q_i<=s_i`; the unrounded gather may be large even when the clipped row is tiny. Metadata operands, divisors, doubled remainders, `M=0`, multiplication workspace and scaled square roots still require explicit admission. Saturating these intermediates is invalid. Round a full exact sum, not an incrementally rounded running mean.

For packed byte widths `u_A,u_Hf,u_N`, a conservative worker reservation for row-major replay is

```text
L F d u_A + d sum_f u_Hf + O(L d u_N)
  + metadata + mean-division/normalization scratch + bounded I/O + runtime.
```

This is sufficient, not minimal. The lead draft supplies a better lifetime: convert each current sum cell to a charged file, release the wide sum arena, then load its compact replacement. For uniform widths and `L>=1`, the history/conversion payload becomes `F*d*max((L-1)*u_A+u_H,L*u_A)`, with approximately `2*L*F*d*u_A` extra conversion write/read bytes. The fair exact-sum baseline reuses its accumulator as the newest history and needs `L*F*d*u_S`, not an extra plane. One temporary `F*d` exact-sum layer still remains. When `L` is small or that layer dominates, small retained means may not make the job fit. Replay remains quadratic in depth in arithmetic; neither quantization nor range clipping removes memberships, repeated passes, or integer division costs.

**Equal-error comparison matters.** At the same node grid, exact-sum lattice replay has coefficient `1/2`, versus worst-case `3/2` for integer means. Two extra node fractional bits make the latter's raw bound no worse, but multiply integer `M` by four. On size-two features this produces a mean-code reservation one bit larger than the feature-local exact sum. At `p=1`, those means are already exact but merely reproduce the original sum code. Extra `p` cannot reduce the irreducible worst-case node-rounding allowance below `Delta/2`.

**Packing can erase an apparent win.** At `M=2^24` and `t_max=24`, the conservative feature-local exact-sum width is 31 bits; the zero-bit mean width is 27. Both fit a 32-bit cell. Comparing only against an `n=106` uniform 33-bit sum reservation suggests a byte saving that this stronger comparator already removes. These are scalar width calculations, not a rerun of the previous prime fixture. Bit-packing narrower than machine words requires its own measured packing/access cost.

Retained disk includes prepared memberships/cardinalities, mean histories, any width directory, output and pinned old generations across the portfolio. Temporary sums may be transient RAM, not free RAM. Preparation, retries, failed staged output, exact/finer fallbacks, refresh and client backpressure all count. A change in membership cardinality changes both the quantization scale and reconstruction multiplier: do not combine old means with new `t_f`, degrees or weights. Use the original decimal 4 GB physical/50 GB retained constraints; no packed lifecycle proof is established by this review.

## Five Focused Scalar Tests

This is the entire independent probe: exact rational scalar checks, no graph replayer, node-layer engine, lead imports, or old-suite invocation. The third test exhausts a small residue domain, not graph histories. Each integer feature sum in the tightness test has the explicit legal completion described above.

```python
from fractions import Fraction as F
from math import gcd

def check_required_clipping_witness():
    mean = round(F(5, 3))
    unbounded = round(F(3*mean+5, 2))
    assert mean == 2 and unbounded == 6
    assert min(5, max(-5, unbounded)) == 5

def check_tight_local_witnesses():
    cases = [(0,2,(-1,3),(5,3)), (1,4,(3,7),(7,1)),
             (2,8,(7,11),(3,5))]
    for p,t,sums,weights in cases:
        code = [round(F((1<<p)*h,t)) for h in sums]
        degree = sum(weights)*(t-1)
        exact = F(sum(w*h for w,h in zip(weights,sums)),degree)
        perturbed = F(t*sum(w*a for w,a in zip(weights,code)),
                      (1<<p)*degree)
        bound = F(1,2)+F(1,(1<<(p+1))-1)
        assert perturbed == F(3,2) and round(perturbed) == 2
        assert abs(round(perturbed)-exact) == bound
        assert 2 <= max(map(abs,sums))

def check_residue_quantization_bounds():
    checks = 0
    for p in range(5):
        for t in range(2,33):
            reduced = t//gcd(t,1<<p)
            predicted = F(reduced//2,reduced*(1<<p))
            errors = [abs(F(round(F((1<<p)*h,t)),1<<p)-F(h,t))
                      for h in range(2*t)]
            assert max(errors) == predicted
            gain = t*predicted/(t-1)
            assert gain <= F(1,(1<<(p+1))-1)
            if p >= 1:
                assert gain < F(1,2)
            checks += len(errors)
    assert checks == 5270
    print('residue scalar cases:',checks)

def check_zero_normalization_failure():
    mean = round(F(1,2))
    first = round(2*mean-1)
    second = round(2*mean)
    assert (first,second) == (-1,0)
    assert first != 0 and second != 1
    code = round(F(2,2))
    assert (round(F(2*code-2,2)),round(F(2*code,2))) == (0,1)

def check_matched_storage_failure():
    maximum = 1<<24
    local_sum = 2+(24*maximum).bit_length()
    mean_bits = 2+maximum.bit_length()
    assert (local_sum,mean_bits) == (31,27)
    assert local_sum <= 32 and mean_bits <= 32
    assert 2+(4*maximum).bit_length() == 1+2+(2*maximum).bit_length()
    for total in (-3,-1,0,1,3):
        assert round(F(2*total,2)) == total

for test in (check_required_clipping_witness, check_tight_local_witnesses,
             check_residue_quantization_bounds, check_zero_normalization_failure,
             check_matched_storage_failure):
    test()
print('five focused scalar test groups passed')
```

Execution receipt: the fenced block was extracted with `awk` and executed with `python3 -`; exit 0. Output was `residue scalar cases: 5270` followed by `five focused scalar test groups passed`. No lead suite was run. No time, RSS, packed-byte or end-to-end completion claim follows from these scalar tests.

## Lead Manuscript Check

The inspected mean-history recurrence, range proof, conservative `p=0,1,2` error coefficients and normalized gate agree with the contract above. The sharper coefficients here are optional improvements, not fixes to an invalid theorem. The draft's clipping warning is necessary for the general family; the `p>=1` no-op result narrows its scope. The division reservation includes the `M=0` case. For active features, `s_max>=t_max-1`, so its `3*K*s_max*M` numerator reservation also covers a single `K*H` conversion when `M>=1`; arbitrary-precision implementation scratch remains additional.

The matched payload calculation is internally consistent: exact sums at `b=24` use five-byte cells; means at `b=26,p=0` use four-byte retained cells and five-byte temporary sums. With `F*d=6.4 million,L=8`, peaks are `256 MB` and `211.2 MB`, a `17.5%` reduction, plus `409.6 MB` conversion traffic. The mean plan has a no-worse worst-case raw bound for the same real initialization. It does **not** thereby have identical normalized acceptance. These are uniform reservations based on `t_max<=1024`, not a proof against optimal feature-local packing or a measured 4 GB execution.

The draft's Verification State records exit 0 for 180 configurations, 13,779 raw-coordinate checks and 1,101 certified rows, plus eight SHA fixtures with 3,160 checks and 79 certified rows. These are **lead-reported receipts, not independent reruns**. The controlled fixtures' constant coordinate helps their norm floor; the separate SHA fixtures are a useful additional check, not an acceptance-rate estimate.

Completion update: the lead now reports exit 0 for the first observed-defect execution: three exact coarse-grid jobs certified, including seven-step alternating K2 rejected by the universal gate; 24 additional controlled jobs certified 156 rows and staged 3,744 bytes. Missing-delta and missing-eta mutations each falsely accepted wrong output, while the normal gate rejected both. These receipts support the intended eligibility distinction and the necessity of both defect terms. They do not establish production memory, broad acceptance rates, or execution of subsequent coefficient/probe revisions. No lead fence was rerun for this review.

## Brief Adaptive-Certificate Challenge

**Conditional approval of the mathematics.** In node-lattice units, let `delta_t` bound every mean reconstruction error at layer `t`, and let `eta_t` bound every pre-clipping nearest-integer error for transition `t -> t+1`. With `u_t` the pre-round reconstruction,

```text
||u_t-P X_t||max <= 2 delta_t,
||clip(RNE(u_t))-P X_t||max <= 2 delta_t+eta_t,
epsilon_0=0; epsilon_(t+1)=epsilon_t+2 delta_t+eta_t.
```

Stochastic nonexpansion proves `||Delta X_t-P^t R||max<=Delta*epsilon_t`. These maxima require only per-depth scalars, not a node-error plane. Measure all transitions needed by a contributing layer, even when an intermediate layer's output weight is zero. Ceil each rational defect onto an admitted dyadic grid before taking maxima; exact integer comparisons avoid denominator-LCM growth. The stored scalar widths and exact norm-square scratch still need admission.

Five sanity conditions matter:

1. **Zeros are layer-specific.** It is `epsilon_t=0`, not merely `epsilon_0=0`, that proves all candidate zeros at depth `t` are true zeros. For positive `epsilon_t`, retain an uncertified candidate-zero row as `h_t=0` or a refusal flag. Only independently known true zeros may be omitted. The inspected draft explicitly retains this condition; its empty-minimum case must mean no uncertified relevant rows, not a skipped scan.
2. **The norm gate stays global and conservative.** For positive `epsilon_t`, require `h_t>c*epsilon_t` and use `beta_t=2*c*epsilon_t/(h_t-c*epsilon_t)`. For zero `epsilon_t`, set beta to zero even if some rows have zero norm. A single tiny row can defeat this global gate while other rows pass a local certificate. Neither raw max-error control nor clipping preserves positivity, mass, or nonzero status.
3. **Publication error must certify the actual bytes.** The numeric allowance must enclose the whole weighted candidate output, including signed weights, normalization, summation and final finite-format conversion. One maximum over all rows and coordinates is sufficient for an entrywise tolerance and preserves O(L) added certificate state. Keeping separate maxima for all coordinates instead uses O(L+d) scalars. Do not use only an estimate of square-root error.
4. **Do not overstate the comparison.** Upward dyadic rounding with precision at least `p+1` preserves the caps `delta_t<=1/(2K), eta_t<=1/2`, so this gate's raw radius is no worse than `t*(1/2+1/K)`. It need not beat a graph-specific `gamma<2` bound or the sharper cardinality bounds above. The draft's phrase "universal bound" should mean the coarse universal coefficient, not every available prior certificate. Keep independently justified gates as alternatives.
5. **Staging is part of correctness and cost.** Delta measurement, final replay and publication must share one immutable metadata/history/precision version. All maxima and minima must be complete before publishing a complete-result manifest. Failed staged files, output backpressure, conversion files, pinned generations and cleanup remain charged; the small scalar certificate does not shrink output itself.

The proposed K2 witness is sound without another experiment: with initial `(1,-1)`, one feature and `p=b=0`, the mean stays zero and updates are exact sign swaps, so all measured defects vanish. The adaptive gate can certify those rows when the numeric publication allowance fits, while a positive a priori radius refuses. K2 `(1,0)` is not rescued: its mean defect is `1/2`, although node-division rounding is zero. Conversely, K3 `(1,1,-2)` has exact first mean but inexact node rounding, so eta cannot be dropped. These algebraic sanity checks add no new test group or lead implementation.

**Final implementation sanity check, inspection only.** The current second fence obtains eta before clipping, consumes all replay layers even when their output weights are zero, leaves uncertified computed-zero rows in the norm minimum, and measures the returned binary64 value against both candidate interval endpoints. Its certificate accumulates transition defects before deciding whether the current layer contributes to output. Those details match the proof and avoid the principal omissions above. The staged file is assessed only after the final row; a `passed` boolean in this probe is not itself a production atomic-publication or cleanup implementation. No new correctness blocker was found in this bounded gate inspection.

The extension is a useful additional eligibility certificate, not a novel residual theorem, generic completion proof, or packed lifecycle measurement. The same-factor competitor can collect the same scalars and receive the same benefit.

## What Remains

The useful delta is an explicitly certified exchange of retained-history range for quantization error, sharpened by exact feature cardinalities. The strongest remaining challenge is to show useful complete normalized outputs and a lifecycle win over feature-local exact widths, mixed scaled-sum histories and external node-state propagation. Quantizing histories, using means to control magnitude, and choosing precision with aggregation in mind are already represented in the inspected art. Separation from a generic quantized-factor replay executor and publication novelty remain unresolved.

Review status: complete within the assigned scope. No additional implementation, lead-suite execution, or shared-file change is pending from this reviewer.

# Embeddings Through Clipped Feature Means

Date: 2026-09-20. A06 candidate extension. Mathematical derivation plus a new falsifiable probe, not a physical-memory benchmark or an established publication contribution.

## Premise Check And Expert Lenses

[Integer lattice replay](Embeddings-Lattice-History-Replay.md) removes degree-LCM growth, but an exact feature sum still needs a scalar range proportional to feature cardinality. Keeping many layers of those sums can dominate the worker. The next question is whether the **retained** object must be a sum at all.

The proposed answer is no, under a declared approximate-output contract. Store a quantized feature mean; transiently form an exact sum while building each layer. Reconstruct a node using feature cardinalities and its own preceding value, then clip to the proved initial range. This replaces cardinality-dependent retained widths by range-dependent widths. It does not eliminate cardinality-dependent accumulator/divisor workspace, the number of features, quadratic-depth replay, or near-zero normalization failures.

Expert lenses: stochastic-operator stability; external-memory lifetime accounting; quantization and precision allocation; skeptical same-factor comparison. The user problem is a full embedding job on a small machine, not merely a smaller logical vector.

## Candidate Approaches And Chosen Thesis

| Approach | Benefit | Paid Limitation |
|---|---|---|
| Exact integer sums | Half-grid local error; simple cancellation | Each retained scalar can need log(t_f) extra bits |
| Quantized means with clipping | Smaller retained range; deterministic error bound | Additional rounding, conversion I/O, possible refusal |
| Disk-resident node plane | Avoid replay; stream node state each layer | n*d traffic/state, output accumulation and disk allowance |
| Validated floating histories | Data-dependent error may be much tighter | Reduction/reconstruction certificates and exponent ranges |

The chosen thesis is a **time-space-accuracy frontier**, not a claim that quantization itself is new. The strongest control gets the same incidence representation and can use the same quantized means. No separation against that control is established.

## Operator And Two Precision Knobs

Use the exact A06 target and initialization from the lattice manuscript: binary membership B, positive integer feature weights w, t_f>=2, q_i=sum_f B_if*w_f, s_i=sum_f B_if*w_f*(t_f-1), P_ij=sum_f B_if*w_f*B_jf/s_i for j!=i, zero diagonal, and zero rows for isolates. The target is E_t=P^t R followed by the declared dyadic weighted sum of row-normalized layers. It is not a trained GNN or bitwise GDS simulation.

Let R=Delta*z exactly, Delta=2^-b, integer |z|<=M. Choose p>=0 extra fractional bits for retained means, K=2^p. The two integer recurrences are

```text
e_(i,0) = z_i
H_(t,f) = sum_i B_if * e_(i,t)             [temporary exact sum]
a_(t,f) = RNE(K * H_(t,f) / t_f)          [retained mean integer]

e_(i,t+1) = clip[-M,M](RNE(
    (sum_f B_if*w_f*t_f*a_(t,f) - K*q_i*e_(i,t)) / (K*s_i)
))                                        [nonisolates]
e_(i,t+1) = 0                              [isolates]
```

RNE is exact nearest-integer, ties-to-even, including negative inputs. All numerator arithmetic is exact within admitted widths. The output represents Delta*e, not Delta*a. Metadata and every history belong to one immutable snapshot. Duplicate memberships, stale cardinalities or saturating arithmetic invalidate the procedure.

Build a_t by replaying each node from z using frozen a_0,...,a_(t-1), adding its current row into exact H_t, and only then dividing each complete feature sum. Never round each incoming membership contribution, and never consume a partly built history. The final pass replays one node and accumulates all requested normalized layers locally. The replay induction is the same as the sum route, but the full-state reference now executes this **mean-quantized, clipped** update, not ordinary round(P*e).

## Theorem: Range And Finite-Horizon Error

For every coordinate, |e_it|<=M and |a_tf|<=K*M. The first bound follows directly from clipping and valid initialization. The mean of values in [-M,M] is in that interval, whose endpoints become integers after multiplication by K; RNE preserves it. Temporary sums satisfy |H_tf|<=t_f*M.

Define gamma=max_(s_i>0)(1+q_i/s_i), with gamma=1 on an all-isolate graph. Active feature cardinalities imply 1<=gamma<=2. Define

```text
kappa_p = 1/2 + gamma/(2*K) <= 1/2 + 1/K
max_(i,j) |Delta*e_t[i,j] - E_t[i,j]| <= t*kappa_p*Delta.
```

**Proof.** The retained mean represents H_tf/t_f with error at most 1/(2K) in node-lattice units. Consequently, before node rounding its reconstructed weighted value differs from (P*e_t)_i by at most

```text
sum_f B_if*w_f*t_f/(2*K*s_i)
  = (s_i+q_i)/(2*K*s_i) <= gamma/(2*K).
```

RNE contributes at most 1/2. The exact comparator (P*e_t)_i lies inside [-M,M]. Projection onto that interval cannot increase its distance to the comparator, so clipping does not add an error term. Finally ||P||_infinity<=1 gives the accumulated bound by induction. This is an application of established nonexpansive quantization analysis, not a new general stability theorem. The bound is valid, not asserted tight.

For p=0 the universal coefficient is 3/2, versus 1/2 for exact feature sums. Two additional node-grid bits make the mean route's bound **no worse** at the same depth and same real initialization: (3/2)*2^-(b+2) <= (1/2)*2^-b. This comparison concerns raw error bounds, not guaranteed equal publication decisions or actual output error.

### Denominator-Aware Sharp Refinement

The independent [mean-history review](Embeddings-Mean-History-Review.md) derives a stronger bound from integer residues. Put g_f=gcd(t_f,K), r_f=t_f/g_f. The possible fractional mean residues imply

```text
mean_error(f,p) <= floor(r_f/2)/(r_f*K)
lambda_i(p) = 1/2 + sum_f B_if*w_f*t_f*mean_error(f,p)/s_i.
lambda_p = 1/2 + 1/(2*K-1) = 3/2, 5/6, 9/14, ... .
```

The feature contribution t_f*mean_error/(t_f-1) is <=1/(2*K) when r_f is odd, and <=1/(2*K-1) when r_f is even. In the latter case g_f=K and t_f>=2K. The node contribution is a convex combination of these quantities. Thus lambda_p is a uniform bound, sharper than kappa_p for p>=1. Use a maximum across all relevant rows, not just the output row's lambda_i: earlier errors can arrive from other nodes.

The optional graph-dependent maximum requires a paid metadata pass and validated arithmetic. Naively summing feature-dependent rational bounds can itself grow denominators; use admitted outward dyadic arithmetic or explicitly price that width. The executable universal gate uses the closed-form lambda_p, not an unimplemented free graph-specific planner.

This constant is universally sharp for one step. Take two size-2K features meeting only at a zero-valued node. Choose their other-member sums H1,H2 to be consecutive integers congruent to 3 modulo 4 around 3*(2K-1)/2-1. Positive integer weights summing to eight realize that weighted mean. Each reconstructed feature sum is H+1; the exact node value is 3/2-1/(2K-1), its reconstructed value is 3/2, and ties-to-even gives two. Error is exactly lambda_p without clipping. The review supplies explicit legal witnesses for p=0,1,2 and a finite residue check. It does not claim the linear-depth bound is attained at every depth.

For p>=1 the pre-node-round mean perturbation is <=1/(2K-1)<1/2. Since the invariant endpoints are integers, nearest-even rounding cannot leave [-M,M]. Clipping is therefore a no-op in this regime; keeping it is harmless. For p=0 the counterexample below still requires it. If every t_f divides K, feature means are exact and only the half-cell node rounding remains. On size-two features at p=1, the mean code is literally the exact sum, so no history compression follows.

The executable universal gate below now uses lambda_p. The original kappa proof remains as the simpler derivation and the bound used by the observed-defect fallback; these are not conflicting output targets. The refinement is a local theorem established by this review, not a global novelty assertion.

### Why Clipping Is Necessary

On one K3 feature, M=5 and e=(-5,5,5), the rounded mean at p=0 is 2. Reconstructing the first node gives RNE((3*2-(-5))/2)=6, outside the assumed range. Clipping returns 5, which is the true neighbor average. Omitting clipping invalidates the range theorem and all repeated-history reservations that depend on it. Clipping is justified by a proved invariant here, not arbitrary model-accuracy tuning.

### The Two Knobs Do Not Give A Free Precision Win

At fixed total retained mean precision q=b+p, the universal physical local error is

```text
(1/2)*2^-b + 2^-(b+p) = (2^(p-1)+1)*2^-q.
```

It is minimized at p=0. The sharper universal coefficient gives `(K/2+K/(2K-1))*2^-q`, also minimized at p=0 for K in {1,2,4,...}; actual graph-specific exact means can be better. Extra mean bits can still lower the temporary node/sum width for a given admitted error target, so byte alignment and the current-layer peak may favor p>0. This is an explicit storage allocation choice, not a claim that more fractional mean bits always improve a fixed-size history. The prototype tests p=0,1,2 against their sharper bounds.

## Normalized Output Gate

Use c=ceil(sqrt(d)), S=sum_j e_j^2 and h=floor(sqrt(S)). A safe row error radius in lattice units is r_t=t*c*lambda_p. When h>r_t,

```text
beta_t = 2*r_t/(h-r_t)
```

bounds the Euclidean difference between the true normalized row and the normalized integer row. For the universal p=0 choice this is 6*t*c/(2*h-3*t*c). Isolates after layer zero, exact layer-zero zero rows, and a separately validated global M=0 case are known zeros. A computed zero at positive depth is otherwise undecided, not a zero certificate.

Enclose normalize(e) by integer square-root and outward dyadic division; expand by beta, multiply by signed dyadic layer weights, and sum endpoints on a common power-of-two grid. Publish a finite binary64 value only if its exact distance to both endpoints is <=tau. Binary32 needs its own conversion check. No norm-floor recipe replaces the actual gate. A0=...=A_L=0 is a direct zero answer; dimension, layer count, scalar exponents and format must still be validated by the adapter.

All rows must certify before a complete-result manifest is published. Failed attempts, staged output and precision retries consume their reserved storage and time. More precise raw values do not fix an output format incapable of representing the requested tolerance.

## Observed-Defect Whole-Output Certificate

The universal bound needlessly refuses some exact coarse-grid trajectories. A second gate can use the errors actually incurred, without retaining an n-by-d error plane. The invariant is still the established forced-error argument; the new local design is its integration with history conversion and staged output.

During conversion of complete H_t to a_t, record one scalar

```text
delta_t = max_(f,j) |t_f*a_tfj - K*H_tfj| / (K*t_f).
```

During the final replay of all nodes, record one scalar per depth

```text
eta_t = max_(i,j) |(K*s_i)*RNE(N_itj/(K*s_i)) - N_itj| / (K*s_i).
```

Here N is the exact pre-clipping numerator already formed by the replay. Isolates contribute zero. Eta must measure nearest-integer rounding **before** clipping; clipping can move a value by more than half a cell but is handled by projection nonexpansion in the theorem. Store delta and eta rounded upward on an admitted dyadic lattice 2^-c_err, with c_err>=p+1. Maxima and ceilings are exact integer operations. This avoids a new LCM from summing many unrelated rational denominators. Retained certificate state is O(L) scalars.

Let epsilon_0=0 and epsilon_(t+1)=epsilon_t+2*delta_t+eta_t, using the upward values. The proof above, with actual mean/rounding defects, yields coordinate error <=Delta*epsilon_t. It never exceeds the coarse universal bound t*(1/2+1/K) because both endpoints 1/(2K) and 1/2 are exactly representable at c_err>=p+1. It need not beat the sharper lambda_p or an admitted graph-specific bound. Replayed histories must be immutable: measurements from a different trajectory do not certify the output.

While staging normalized candidate rows, also collect the minimum h_t=floor(norm(e_t)) over rows that are not independently known to be true zero, and the maximum per-coordinate interval-to-returned-float error of the candidate output. For epsilon_t>0 require h_t>ceil(sqrt(d))*epsilon_t and charge

```text
beta_t = 2*ceil(sqrt(d))*epsilon_t /
         (h_t-ceil(sqrt(d))*epsilon_t).

whole-output error <= candidate_publication_error
                    + sum_t |alpha_t|*beta_t.
```

For epsilon_t=0 the rounded and true layer coincide; a computed zero at that layer is then a justified exact zero and beta_t=0. Empty minima mean every relevant row at that layer is independently known zero. Never omit an uncertified rounded-zero row from the minimum just to pass the gate.

After all rows are staged and all maxima/minima are final, publish only if the total bound is <=tau. This avoids a separate global-error prepass or resident per-row norms by using a conservative global norm floor. It can be weaker than the original per-row gate on heterogeneous norms; retain the two as separately justified alternatives. No success is announced midstream. The complete output file, numeric accumulator widths, dyadic error precision, hashes and failure cleanup remain paid.

**Separating eligibility example, not a novelty/speed claim:** on K2 initialized to (1,-1), p=b=0, the exact trajectory alternates signs. Every retained mean and every node update is exact, so all observed defects are zero. The universal positive-radius gate refuses at positive depth, but this gate certifies complete normalized output. A clique/pair-specialized exact solver also handles it; the example isolates certificate conservatism, not superior graph computation.

**Two required falsifiers:** K2 initialized to (0,1) has mean error but no node-division rounding; dropping delta can certify wrong output. K3 initialized to (1,1,-2) has exact initial mean but inexact node division; dropping eta can certify wrong output. Both terms are necessary. No probabilistic cancellation or independent-roundoff assumption is used.

## Byte And Lifetime Accounting

For uniform packed widths, let u_A be bytes for a mean, u_H bytes for a temporary sum, and u_N bytes for node reconstruction operands. Conservative bit allowances are

```text
mean values        2 + bit_length(max(1,K*M))
temporary sums     2 + bit_length(max(1,t_max*M))
numerator values   2 + bit_length(max(1,3*K*s_max*M))
division operands  2 + bit_length(max(1,3*K*s_max*max(1,M),K*t_max)).
```

The last line covers degree/cardinality divisors even when M=0; arithmetic implementation scratch is additional. Form K*H one cell at a time for mean conversion, not a second F*d wide table. Per-feature exact-sum widths based on each t_f are a stronger control than the crude n*M bound. Mean widths are independent of t_f only **per retained scalar**; total F*d*L state is not independent of graph size.

For L>=1, a memory schedule that streams converted means to a reserved file, releases the wide sum arena, and then loads the packed means has peak history payload

```text
F*d*max((L-1)*u_A + u_H, L*u_A)
```

plus O(L*d*u_N) gathers, metadata, arithmetic/normalization scratch, runtime and I/O pools. Before the last build pass there are L-1 old mean planes and one sum plane; after conversion there are L mean planes. The old exact-sum route can retain its current accumulator as the next history, so the matched peak is **L*F*d*u_S**, not a needlessly duplicated (L+1)-plane baseline.

```text
old packed means ----+                 old means + new means
                     |                          ^
new wide sums --> stream compact file --> release sums --> load
                       [disk charged]           [bounded I/O]
```

This lifetime schedule costs about 2*L*F*d*u_A bytes of conversion write/read traffic. If files are retained for recovery, count their unique blocks in prepared retention. If they are scratch, charge their live lifetime separately. Page-cache residency counts toward physical memory. Truncating a vector's length does not prove its allocation was released. The probe below does not implement this packed-file schedule; its simultaneous Python lists must not be measured as that bound.

Replay still performs O((m+n)*d*L^2) integer work, O(L*F*d) conversions, and O(L) incidence-volume passes with bounded two-cursor gathers/scatters. Cardinality-weight products are extra work. Ordinary grouped linear closure remains invalid under the nonlinear quantization/clipping. A disk-node-plane control trades O(n*d*L) state traffic for less replay and may win decisively.

### Illustrative Matched Reservation, Not A Benchmark

Take n=20 million, F=100,000, d=64, L=8, t_max<=1024, ternary real initialization, exact-sum b=24 and mean b=26,p=0. This is a conditional size calculation, not a dataset or a demonstrated norm floor. A graph with average membership two has m=40 million and average feature cardinality 400; actual incidence, degree diversity and compression must be measured.

| Payload Only | Exact Sums | Clipped Means | Difference |
|---|---:|---:|---:|
| Byte-packed history/conversion peak | 256 MB: 8*6.4M*5 | 211.2 MB: 6.4M*(7*4+5) | 17.5% less |
| 32/64-bit cell version | 409.6 MB | 230.4 MB | 43.75% less |
| Additional conversion write/read | None required by this schedule | 409.6 MB | More I/O |
| One raw node plane, 32-bit control | 5.12 GB | Not retained | External-plane alternative remains |
| Complete float32 values-only output | 5.12 GB | 5.12 GB | No output saving |

IDs, framing, topology, numerical workspace and all lifecycle overlap remain outside this payload table and must be added. A 4 GB physical claim does not follow. The two extra bits in the mean plan are already included, so its raw error bound is no worse than the sum plan's. Small features may need the same cell size in both plans; at L=1 the temporary sum may eliminate the entire saving. Per-feature packing, exact grouped propagation on favorable graphs, and competent general fixed-point replay can narrow or erase the advantage.

## Primary Art And Scientific Boundary

- [Nedic et al., quantized distributed averaging](https://www.mit.edu/~jnt/Papers/J123-09-quant-averaging.pdf), Section V, is the closest stability precedent identified in the completed [lattice review](Embeddings-Defect-Prior-Art.md). Its finite-horizon forcing argument is inherited; its consensus conclusions do not automatically apply to this loop-free operator.
- [SGQuant](https://arxiv.org/pdf/2007.05100), Sections III-A and IV, inspected in primary full text here, already quantizes graph embedding/attention data and selects precision at component, topology and layer granularities. It reconstructs values for computation and uses finetuning. Its evaluated accuracy objective differs from a per-coordinate certificate against a fixed untrained propagation operator. These inspected sections do not give our no-node-plane feature-history schedule, but that observation is not a priority proof.
- A generic factor-aware replayer given a_t implements this exact same schedule. A node-plane comparator can use the same mean quantization and clipping. Claiming victory by requiring either to materialize expanded adjacency would be invalid.

The candidate delta is a specific admitted mean-history/no-node-state composition with a finite-horizon normalized-output certificate and a paid conversion lifetime. Whether that is publishable requires useful complete jobs and a meaningful frontier against strong same-factor, external-state and validated-low-precision controls. Generic quantization and clipping are not the invention.

## New Executable Probe

Run this single Python fence with Python 3.11. Small input and dense oracle tables are deliberate test fixtures. The history builder replays one node and retains feature means, not node layers. The verifier collects trajectories separately to compare every coordinate and normalized result; it is not a production RSS test.

```python
from fractions import Fraction as F
from hashlib import sha256
from math import isqrt, lcm, isfinite
from random import Random

def round_exact_integer_even(num, den):
    assert den > 0
    q,r = divmod(num,den)
    return q + int(2*r > den or (2*r == den and q%2))

def build_mean_fixture_metadata(rows, weights):
    counts = [sum(f in row for row in rows) for f in range(len(weights))]
    assert all(t>=2 for t in counts) and all(w>0 for w in weights)
    assert all(len(row)==len(set(row)) for row in rows)
    q = [sum(weights[f] for f in row) for row in rows]
    s = [sum(weights[f]*(counts[f]-1) for f in row) for row in rows]
    return counts,q,s

def replay_clipped_mean_layers(i, history, initial, rows, weights, counts, q, s, maximum, p, rounding=None, error_bits=32):
    dim, K = len(initial[i]),1<<p
    gathered = [[0]*dim for _ in history]
    for f in rows[i]:
        for t in range(len(history)):
            for j in range(dim):
                gathered[t][j] += weights[f]*counts[f]*history[t][f][j]
    value = initial[i][:]
    yield value
    for t,total in enumerate(gathered):
        updated=[]
        for j in range(dim):
            if not s[i]: updated.append(0); continue
            num,den=total[j]-K*q[i]*value[j],K*s[i]
            raw=round_exact_integer_even(num,den)
            if rounding is not None:
                error=(abs(den*raw-num)*(1<<error_bits)+den-1)//den
                rounding[t]=max(rounding[t],error)
            updated.append(max(-maximum,min(maximum,raw)))
        value=updated
        yield value

def build_clipped_mean_history(rows, weights, initial, depth, p, counts, q, s, deltas=None, error_bits=32):
    if not rows:
        assert not weights and not initial
        return []
    dim,maximum,K = len(initial[0]),max(abs(x) for row in initial for x in row),1<<p
    history = []
    for t in range(depth):
        current = [[0]*dim for _ in weights]
        for i,row in enumerate(rows):
            value = None
            for value in replay_clipped_mean_layers(i,history,initial,rows,weights,counts,q,s,maximum,p):
                pass
            for f in row:
                for j in range(dim): current[f][j] += value[j]
        assert all(abs(x)<=counts[f]*maximum for f,vector in enumerate(current) for x in vector)
        means = [[round_exact_integer_even(K*x,counts[f]) for x in vector]
                 for f,vector in enumerate(current)]
        assert all(abs(x)<=K*maximum for vector in means for x in vector)
        if deltas is not None:
            assert error_bits>=p+1
            error=0
            for f,vector in enumerate(current):
                den=K*counts[f]
                for j,x in enumerate(vector):
                    num=abs(counts[f]*means[f][j]-K*x)*(1<<error_bits)
                    error=max(error,(num+den-1)//den)
            deltas.append(error)
        history.append(means)
    return history

def enclose_normalized_integer_row(value, precision):
    square = sum(x*x for x in value)
    if not square: return [(F(0),F(0)) for _ in value]
    ps = precision+4
    lower,upper = isqrt(square<<(2*ps)),isqrt(square<<(2*ps))+1
    result = []
    for x in value:
        num = x<<(precision+ps)
        dl,du = (upper,lower) if x>=0 else (lower,upper)
        lo,hi = F(num//dl,1<<precision),F(-((-num)//du),1<<precision)
        if x>=0: assert 0<=lo<=hi and lo*lo*square<=x*x<=hi*hi*square
        else: assert lo<=hi<=0 and hi*hi*square<=x*x<=lo*lo*square
        result.append((lo,hi))
    return result

def publish_clipped_mean_row(layers, degree, alpha, tau, p, maximum, precision=40):
    dim = len(layers[0])
    assert len(alpha)==len(layers) and dim>0 and tau>0
    c = isqrt(dim) + (isqrt(dim)**2 < dim)
    kappa,sums = F(1,2)+F(1,(1<<(p+1))-1),[[F(0),F(0)] for _ in layers[0]]
    for t,(value,weight) in enumerate(zip(layers,alpha)):
        if not weight: continue
        known_zero = maximum==0 or (t==0 and not any(value)) or (t>0 and degree==0)
        if known_zero: bounds=[(F(0),F(0)) for _ in value]
        else:
            h,r = isqrt(sum(x*x for x in value)),t*c*kappa
            if h<=r: return None
            beta = 2*r/(h-r)
            margin = F(-((-beta.numerator*(1<<precision))//beta.denominator),1<<precision)
            bounds = [(lo-margin,hi+margin) for lo,hi in enclose_normalized_integer_row(value,precision)]
        for j,(lo,hi) in enumerate(bounds):
            if weight<0: lo,hi=hi,lo
            sums[j][0]+=weight*lo; sums[j][1]+=weight*hi
    answer=[]
    for lo,hi in sums:
        try: value=float((lo+hi)/2)
        except OverflowError: return None
        if not isfinite(value): return None
        exact=F.from_float(value)
        if max(abs(exact-lo),abs(exact-hi))>tau: return None
        answer.append(value)
    return answer

def verify_clipped_mean_fixture(rows, weights, initial, depth, b, p, alpha, tau):
    n,dim,K,scale=len(rows),len(initial[0]),1<<p,1<<b
    counts,q,s=build_mean_fixture_metadata(rows,weights)
    maximum=max(abs(x) for row in initial for x in row)
    history=build_clipped_mean_history(rows,weights,initial,depth,p,counts,q,s)
    replay=[list(replay_clipped_mean_layers(i,history,initial,rows,weights,counts,q,s,maximum,p)) for i in range(n)]
    # Dense operator and all-node planes exist only in the independent verifier.
    matrix=[[sum(weights[f] for f in set(row)&set(other)) if i!=j else 0
             for j,other in enumerate(rows)] for i,row in enumerate(rows)]
    assert list(map(sum,matrix))==s
    exact=[[[F(x,scale) for x in row] for row in initial]]
    state=[initial]
    kappa=F(1,2)+F(1,2*K-1)
    for t in range(depth):
        means=[[round_exact_integer_even(K*sum(state[-1][i][j] for i,row in enumerate(rows) if f in row),counts[f])
                for j in range(dim)] for f in range(len(weights))]
        assert means==history[t]
        next_state=[]
        for i,row in enumerate(rows):
            next_state.append([max(-maximum,min(maximum,round_exact_integer_even(
                sum(weights[f]*counts[f]*means[f][j] for f in row)-K*q[i]*state[-1][i][j],K*s[i])))
                if s[i] else 0 for j in range(dim)])
        state.append(next_state)
        exact.append([[sum(F(matrix[i][h],s[i])*exact[-1][h][j] for h in range(n))
                       if s[i] else F(0) for j in range(dim)] for i in range(n)])
    checked=accepted=refused=0
    for i in range(n):
        for t,value in enumerate(replay[i]):
            assert value==state[t][i]
            for j,x in enumerate(value):
                assert abs(x)<=maximum and abs(F(x,scale)-exact[t][i][j])<=t*kappa/scale
                checked+=1
        answer=publish_clipped_mean_row(replay[i],s[i],alpha,tau,p,maximum)
        if answer is None: refused+=1; continue
        reference=[[F(0),F(0)] for _ in range(dim)]
        for t,weight in enumerate(alpha):
            denominator=lcm(*(x.denominator for x in exact[t][i]))
            integers=[int(x*denominator) for x in exact[t][i]]
            for j,(lo,hi) in enumerate(enclose_normalized_integer_row(integers,80)):
                if weight<0: lo,hi=hi,lo
                reference[j][0]+=weight*lo; reference[j][1]+=weight*hi
        for value,(lo,hi) in zip(answer,reference):
            assert max(abs(F.from_float(value)-lo),abs(F.from_float(value)-hi))<=tau
        accepted+=1
    return checked,accepted,refused,int(refused==0)

assert [round_exact_integer_even(x,2) for x in (-5,-3,-1,1,3,5)]==[-2,-2,0,0,2,2]
unclipped=round_exact_integer_even(3*round_exact_integer_even(5,3)+5,2)
assert unclipped==6 and min(5,unclipped)==5  # Removing clipping violates range M=5.
assert build_clipped_mean_history([],[],[],4,0,[],[],[])==[]
rng,totals=Random(914),[0,0,0,0]
for case in range(60):
    n,dim,depth,b=rng.randrange(3,10),3,rng.randrange(1,6),16
    sets=[]
    for _ in range(rng.randrange(1,7)):
        members={i for i in range(n) if rng.randrange(2)}
        if len(members)>=2: sets.append(members)
    rows=[tuple(f for f,members in enumerate(sets) if i in members) for i in range(n)]
    weights=[rng.randrange(1,9) for _ in sets]
    initial=[[(1 if j==0 else rng.randrange(-2,3))*(1<<b) for j in range(dim)] for _ in rows]
    alpha=[F(-1,4) if t%2 else F(1,2) for t in range(depth+1)]
    for p in (0,1,2):
        result=verify_clipped_mean_fixture(rows,weights,initial,depth,b,p,alpha,F(1,100))
        for j in range(4): totals[j]+=result[j]
print('controlled_mean',dict(configurations=180,coordinate_checks=totals[0],certified_rows=totals[1],refused_rows=totals[2],complete_jobs=totals[3]))

sha_totals=[0,0,0,0]
for seed in range(8):
    n,dim,depth,b=9+seed%3,8,4,24
    sets=[{i for i in range(n) if (i+f)%4!=0} for f in range(4)]
    rows=[tuple(f for f,members in enumerate(sets) if i in members) for i in range(n)]
    initial=[]
    for i in range(n):
        vector=[]
        for j in range(dim):
            u=int.from_bytes(sha256(b'DFHR-v1'+seed.to_bytes(32,'little')+i.to_bytes(8,'little')+j.to_bytes(4,'little')).digest()[:8],'little')
            z=0 if u&1 else (-1 if (u>>1)&1 else 1)
            vector.append(z*(1<<b))
        initial.append(vector)
    result=verify_clipped_mean_fixture(rows,[1,2,3,5],initial,depth,b,0,[F(1,4)]*5,F(1,1000))
    for j in range(4): sha_totals[j]+=result[j]
print('sha_mean',dict(configurations=8,coordinate_checks=sha_totals[0],certified_rows=sha_totals[1],refused_rows=sha_totals[2],complete_jobs=sha_totals[3]))

rows=[(0,)]*3
bad=verify_clipped_mean_fixture(rows,[1],[[0],[1],[1]],1,0,0,[F(0),F(1)],F(1,1000))
good=verify_clipped_mean_fixture(rows,[1],[[0],[1<<20],[1<<20]],1,20,0,[F(0),F(1)],F(1,1000))
assert bad[2]==3 and good[3]==1
zero=verify_clipped_mean_fixture([(0,),(0,)],[10**100],[[0,0],[0,0]],3,0,0,[F(1,4)]*4,F(1,1000))
assert zero[3]==1
depth_zero=verify_clipped_mean_fixture([(),()],[],[[8,0],[0,0]],0,3,0,[F(1)],F(1,1000))
assert depth_zero[3]==1
cells,depth=100000*64,8
sum_peak=depth*cells*5
mean_peak=cells*max((depth-1)*4+5,depth*4)
assert (sum_peak,mean_peak)==(256000000,211200000)
assert F(sum_peak-mean_peak,sum_peak)==F(7,40)
assert F(3,2)*(F(1,2)**26)<=F(1,2)*(F(1,2)**24)
print('boundaries clip_mutation=detected; same_real_refinement=passed; M0_large_divisor=passed; n0_L0=passed')
print('reservation payload_bytes',sum_peak,mean_peak,'reduction_fraction',F(7,40))
```

## Verification State

The initial fence execution exited 0. It checked 180 configurations on 60 controlled graph fixtures across p=0,1,2: 13,779 raw coordinates, 1,101 certified rows and 180 complete jobs. Eight SHA-profile fixtures added 3,160 coordinates, 79 certified rows and eight complete jobs. The clipping mutation was detected; the same real input refused on a coarse grid and completed on the finer grid. Empty/depth-zero and all-zero/large-divisor checks passed. The 256 MB / 211.2 MB payload calculation was checked as integer arithmetic, not measured allocation.

These controlled fixtures include a constant initialized coordinate; the separate SHA fixtures do not. Neither set establishes real-workload acceptance probability. The first observed-defect execution also exited 0: three exact coarse-grid jobs certified, including an alternating seven-step K2 for which the universal gate refused both rows; 24 new controlled jobs certified 156 rows and staged 3,744 binary64 bytes. Dropping either mean or division defects falsely certified an incorrect fixture, and the unmutated gate refused both. This validates the tested gate, not a physical memory cap.

The final focused execution after adopting the sharper lambda_p and adding raw observed-radius checks exited 0 with all preceding counts unchanged. The 24 observed-defect jobs additionally checked 1,872 raw coordinates against their measured dyadic error radii and the dense rational target. Output length checks covered complete staged rows, and intermediate output weights of zero did not suppress transition-error measurement. The [independent review](Embeddings-Mean-History-Review.md) supports the mean theorem, supplies its sharp refinement and conditionally supports the adaptive gate under the documented accounting/publication conditions. Its five scalar test groups and 5,270 residue cases are separate review evidence, not lead reruns.

### Observed-Defect Extension Probe

This second fence uses the definitions above. It stages complete binary64 output in a temporary file and keeps only per-depth certificate scalars plus one node's local computation. The input and small dense oracle remain test fixtures; Python object widths and physical memory are not bounded by this program. Extract both fences in order to run.

```python
import struct
import tempfile

def certify_observed_error_budget(deltas, rounding, minima, alpha, dim, numeric_error, tau, error_bits=32):
    assert len(deltas)==len(rounding)==len(alpha)-1
    assert len(minima)==len(alpha) and dim>0
    c=isqrt(dim)+(isqrt(dim)**2<dim)
    total,epsilon= numeric_error,0
    for t,weight in enumerate(alpha):
        if t: epsilon+=2*deltas[t-1]+rounding[t-1]
        if not weight or epsilon==0 or minima[t] is None: continue
        r=F(c*epsilon,1<<error_bits)
        if minima[t]<=r: return False,None
        total+=abs(weight)*2*r/(minima[t]-r)
    return total<=tau,total

def publish_observed_defect_rows(stage, rows, weights, initial, depth, p, alpha, tau, error_bits=32):
    assert depth>=0 and p>=0 and error_bits>=p+1 and len(alpha)==depth+1 and tau>0
    if not rows:
        assert not weights and not initial
        return True,dict(bytes=0)
    counts,q,s=build_mean_fixture_metadata(rows,weights)
    dim,maximum=len(initial[0]),max(abs(x) for row in initial for x in row)
    assert dim>0 and len(initial)==len(rows) and all(len(row)==dim for row in initial)
    deltas=[]
    history=build_clipped_mean_history(rows,weights,initial,depth,p,counts,q,s,deltas,error_bits)
    rounding,minima=[0]*depth,[None]*(depth+1)
    numeric_error=F(0)
    for i in range(len(rows)):
        sums=[[F(0),F(0)] for _ in range(dim)]
        layers=replay_clipped_mean_layers(i,history,initial,rows,weights,counts,q,s,maximum,p,rounding,error_bits)
        for t,(value,weight) in enumerate(zip(layers,alpha)):
            if not weight: continue
            known_zero=maximum==0 or (t==0 and not any(value)) or (t>0 and s[i]==0)
            if not known_zero:
                h=isqrt(sum(x*x for x in value))
                minima[t]=h if minima[t] is None else min(minima[t],h)
            for j,(lo,hi) in enumerate(enclose_normalized_integer_row(value,40)):
                if weight<0: lo,hi=hi,lo
                sums[j][0]+=weight*lo; sums[j][1]+=weight*hi
        candidate=[]
        for lo,hi in sums:
            try: value=float((lo+hi)/2)
            except OverflowError: return False,dict(bytes=stage.tell(),reason='output-format')
            if not isfinite(value): return False,dict(bytes=stage.tell(),reason='output-format')
            numeric_error=max(numeric_error,abs(F.from_float(value)-lo),abs(F.from_float(value)-hi))
            candidate.append(value)
        stage.write(struct.pack('<'+'d'*dim,*candidate))
    passed,bound=certify_observed_error_budget(deltas,rounding,minima,alpha,dim,numeric_error,tau,error_bits)
    return passed,dict(bytes=stage.tell(),deltas=deltas,rounding=rounding,minima=minima,numeric=numeric_error,bound=bound)

def verify_observed_output_target(rows, weights, initial, depth, b, p, alpha, tau):
    with tempfile.TemporaryFile() as stage:
        passed,receipt=publish_observed_defect_rows(stage,rows,weights,initial,depth,p,alpha,tau)
        n,dim=len(rows),len(initial[0])
        counts,q,s=build_mean_fixture_metadata(rows,weights)
        matrix=[[sum(weights[f] for f in set(rows[i])&set(rows[j])) if i!=j else 0 for j in range(n)] for i in range(n)]
        exact=[[[F(x,1<<b) for x in row] for row in initial]]
        for t in range(depth):
            exact.append([[sum(F(matrix[i][k],s[i])*exact[-1][k][j] for k in range(n)) if s[i] else F(0)
                           for j in range(dim)] for i in range(n)])
        history=build_clipped_mean_history(rows,weights,initial,depth,p,counts,q,s)
        maximum=max(abs(x) for row in initial for x in row)
        epsilon=[0]
        for delta,eta in zip(receipt['deltas'],receipt['rounding']):
            epsilon.append(epsilon[-1]+2*delta+eta)
        raw_checks=0
        for i in range(n):
            for t,value in enumerate(replay_clipped_mean_layers(i,history,initial,rows,weights,counts,q,s,maximum,p)):
                for j,x in enumerate(value):
                    assert abs(F(x,1<<b)-exact[t][i][j])<=F(epsilon[t],1<<(32+b))
                    raw_checks+=1
        receipt['raw_checks']=raw_checks
        stage.seek(0)
        maximum_error=F(0)
        for i in range(n):
            candidate=struct.unpack('<'+'d'*dim,stage.read(dim*8))
            target=[[F(0),F(0)] for _ in range(dim)]
            for t,weight in enumerate(alpha):
                den=lcm(*(x.denominator for x in exact[t][i]))
                integers=[int(x*den) for x in exact[t][i]]
                for j,(lo,hi) in enumerate(enclose_normalized_integer_row(integers,80)):
                    if weight<0: lo,hi=hi,lo
                    target[j][0]+=weight*lo; target[j][1]+=weight*hi
            for value,(lo,hi) in zip(candidate,target):
                error=max(abs(F.from_float(value)-lo),abs(F.from_float(value)-hi))
                maximum_error=max(maximum_error,error)
                if passed: assert error<=tau
        assert stage.read()==b'' and receipt['bytes']==n*dim*8
    return passed,receipt,maximum_error

tau=F(1,1000000)
pair=verify_observed_output_target([(0,),(0,)],[1],[[1],[-1]],7,0,0,[F(0)]*7+[F(1)],tau)
assert pair[0] and pair[1]['deltas']==[0]*7 and pair[1]['rounding']==[0]*7
assert verify_clipped_mean_fixture([(0,),(0,)],[1],[[1],[-1]],7,0,0,[F(0)]*7+[F(1)],tau)[2]==2
constant=verify_observed_output_target([(0,)]*3,[3],[[1,0]]*3,4,0,0,[F(1,8)]*5,tau)
assert constant[0]
mixed=verify_observed_output_target([(0,),(0,),(1,),(1,),()],[1,2],[[1],[-1],[0],[0],[1]],4,0,0,[F(0)]*4+[F(1)],tau)
assert mixed[0]

bad_mean=verify_observed_output_target([(0,),(0,)],[1],[[0],[1]],1,0,0,[F(0),F(1)],tau)
bad_round=verify_observed_output_target([(0,)]*3,[1],[[1],[1],[-2]],1,0,0,[F(0),F(1)],tau)
assert not bad_mean[0] and not bad_round[0] and bad_mean[2]>tau and bad_round[2]>tau
for result,drop in ((bad_mean,'deltas'),(bad_round,'rounding')):
    receipt=result[1]
    deltas=[0] if drop=='deltas' else receipt['deltas']
    rounding=[0] if drop=='rounding' else receipt['rounding']
    forged,_=certify_observed_error_budget(deltas,rounding,receipt['minima'],[F(0),F(1)],1,receipt['numeric'],tau)
    assert forged  # Missing either measured defect would falsely certify wrong staged values.
print('observed_defect exact_coarse_jobs=3; universal_pair_refusals=2; missing_defect_mutations=2_detected')

observed_rng,observed_count=Random(721),[0,0,0,0]
for case in range(24):
    n,dim,depth,b=5+case%4,3,3,18
    sets=[{i for i in range(n) if (i+f)%3!=0} for f in range(3)]
    rows=[tuple(f for f,members in enumerate(sets) if i in members) for i in range(n)]
    initial=[[(1 if j==0 else observed_rng.randrange(-1,2))*(1<<b) for j in range(dim)] for _ in rows]
    passed,receipt,_=verify_observed_output_target(rows,[1,3,5],initial,depth,b,case%3,[F(1,4)]*4,F(1,1000))
    assert passed
    observed_count[0]+=1; observed_count[1]+=n; observed_count[2]+=receipt['bytes']
    observed_count[3]+=receipt['raw_checks']
print('observed_random',dict(complete_jobs=observed_count[0],certified_rows=observed_count[1],staged_bytes=observed_count[2],raw_checks=observed_count[3]))
```

## Next Scientific Test

After finite correctness checks, compare a complete source-native overlapping-feature job under equal byte caps: packed exact sums, packed means, generic same-mean replay, and a disk-node-plane executor. Vary feature cardinality, depth, precision and near-zero prevalence. Measure full build/output and failed attempts. The mechanism is defeated as a useful contribution if precision/replay/conversion costs erase the resource frontier or the supported outputs mostly refuse. It is not rescued by reporting only history-cell compression.

# Embeddings Through Integer Lattice Replay

Date: 2026-09-20. A06 numerical/storage alternative. A new derivation and finite executable probe; publication novelty and physical resource validation are not established.

## Premise And Chosen Approach

[The existing embedding manuscript](Replay-Embeddings.md) eliminates node-by-dimension intermediate layers, but its exact grouped completion route can require scalar width proportional to `L*log Q`, where Q is a common reduced degree denominator. Small logical state need not mean small bytes. Blind interval propagation through the signed diagonal-plus-feature representation can also overestimate error severely.

This alternative keeps the same exact mathematical target, but computes an explicitly certified approximation on a fixed integer grid. Reconstruct each node's rounded trajectory from exact integer feature sums, round only after completing the self-excluded weighted numerator, and certify normalized output against the original real-valued powers. **No common degree denominator is constructed.** The crucial contraction is that of the original nonnegative row-stochastic operator, not the absolute values of its signed factors.

The tradeoff is explicit: give up the grouped linear closure and retain the original O(L^2) replay work, while bounding scalar width independently of degree-LCM growth. Normalization near zero remains a real obstruction. This is not permission to label a rounded zero as an exact zero or to return an incomplete output as success.

The conventional alternatives are full node-vector fixed-point propagation and exact grouped feature history. Other options are wider interval replay, residual-based certification of arbitrary histories, and this integer-lattice replay. The selected route provides a simple constructive width bound and reproducible rounding semantics. Expert lenses are numerical stability, structured graph computation, bounded memory and adversarial normalization.

## Fixed Contract

Use the same binary incidence, positive integer weights, self-excluded shared-count graph, isolates, stable initialization and output combination as A06:

```text
W_ij = sum_f w_f B_if B_jf for i != j; W_ii=0
q_i = sum_f w_f B_if
s_i = sum_f w_f B_if (t_f-1)
P_ij = W_ij/s_i for s_i>0; isolate rows are zero
E_0 = R; E_(t+1)=P E_t
Y_i = sum_t alpha_t * normalize(E_t[i,:]), normalize(0)=0.
```

Active features have at least two members, so `q_i<=s_i`. Normalization is only in the output, not inside propagation. Layer weights are exact finite dyadics. The returned finite binary32/64 coordinates must satisfy the original per-coordinate absolute tolerance tau. We do not promise GDS bitwise parity, arbitrary weighted cosine, training quality or a stochastic-projection distortion theorem.

Choose grid step Delta=2^-b, b>=0, and require exact grid-aligned initialization `R_ij=Delta*z_ij`, z integer. Default ternary initialization has this property at every b. Optional dyadic node scaling may require a larger b; the exponent and integer magnitude must be admitted. Let `M=max |z_ij|`, obtained from a validated bound or paid scan. Non-grid initialization needs a separately included initial error, not silent rounding under this zero-initial-error theorem.

An empty node universe publishes an empty result before allocating history or querying an initialization row. Depth zero needs no propagation history and only normalizes the exact initial rows. These are boundary cases, not graph-compression successes.

Round rational x to the nearest integer with ties to even, including negative ties. Implement with exact quotient/remainder, not floating division. Metadata sums, membership uniqueness and integer widths are validated before use; saturation and wrapping are forbidden.

## Rounded Operator Without Node Layers

Define integer raw rows e and integer feature history H:

```text
e_(i,0) = z_i
H_(t,f) = sum_i B_if * e_(i,t)
e_(i,t+1) = round_even((sum_f B_if*w_f*H_(t,f) - q_i*e_(i,t))/s_i)
```

For an isolate, every layer after zero is identically zero. Every coordinate shares this formula. Its numerator is formed exactly before the one rounding operation. Rounding each feature contribution separately is a different algorithm and invalidates the stated half-grid error bound.

To build H_t, replay the single current node from z_i using frozen H_0,...,H_(t-1), then add its integer row exactly to each incident feature's new accumulator. Freeze only after the complete node pass. To publish, replay one node through all layers, normalize and accumulate its full row locally, then stage its output. All nodes see the same frozen history and tie rule on every replay.

The row gather can use a t-by-d integer slab: scan one bounded membership cursor, add each feature's contribution to every required history row, then execute local rounds. A second bounded cursor scatters into the current feature sums. It does not retain a high-degree node's entire feature list or any n-by-d layer.

### Theorem 1: Exact Replay Of A Rounded Full-State Algorithm

The reconstructed rows are exactly the same integers as the competent full-state algorithm `e_(t+1)=round_even(P*e_t)`, which forms each exact weighted numerator and rounds once.

**Proof.** Exact feature accumulation gives `sum_f w_f B_if H_tf-q_i e_it = sum_(j!=i) W_ij e_jt`. The deterministic division/rounding is therefore the full-state update. By induction every replay recreates the same e_t, so a newly aggregated H_t is exactly its feature sum. No rounded coefficient, approximate scatter or distinct reduction order can change an integer sum within its admitted width.

### Theorem 2: Range And Raw Error

For every integer coordinate and layer, `|e_it|<=M`. Each nonisolate update averages values in [-M,M]; nearest-integer rounding remains in that interval because its endpoints are integers. Isolates map to zero.

Let `Ehat_t=Delta*e_t`. Then

```text
max_(i,j) |Ehat_t[i,j]-E_t[i,j]| <= t*Delta/2
||Ehat_t[i,:]-E_t[i,:]||_2 <= sqrt(d)*t*Delta/2.
```

**Proof.** Write the rounded update as `Ehat_(t+1)=P Ehat_t+rho_t`, where each coordinate of rho has magnitude at most Delta/2. P is nonnegative with row sum at most one, so its induced infinity norm is at most one. Induction gives the first bound and the coordinate-to-vector inequality gives the second. This familiar nonexpansive error argument is not a new general numerical theorem.

The signed decomposition has `|D|+|UV^T|` row sum `1+2q_i/s_i`, potentially three. Using that matrix's powers as the only error bound can introduce exponential pessimism. Exact cancellation before rounding lets this proof use P itself. Do not infer that every carefully correlated interval implementation suffers the weaker bound.

## Widths And Admitted Work

For the original integer metadata and M, conservative signed widths are

```text
local row          2 + bit_length(max(1,M))
feature history    2 + bit_length(max(1,n*M))
weighted numerator 2 + bit_length(max(1,3*s_max*M))
division operands  2 + bit_length(max(1,3*s_max*max(1,M))).
```

Indeed each H_tf is bounded by t_f*M. A partial gathered sum has absolute magnitude at most `sum_f B_if*w_f*t_f*M=(s_i+q_i)M`. Adding the diagonal term gives at most `(s_i+2q_i)M<=3s_iM`, including conservative intermediate states. For M>=1 the displayed value bound also covers the divisor and doubled remainder. For M=0 it does not: zero numerators can coexist with arbitrarily large degrees. Admit the separate operand width above, or take a separately validated all-zero fast path. Define s_max=0 on empty/all-isolate inputs. Integer multiplication/division workspace and metadata are additional reservations, not consequences of a result-value width. These are bit-width bounds, not Python object sizes.

No Q, Q^L, exact rational node trajectory or n-norm table is retained. Main logical memory is `L*F*d` history integers plus O(L*d) local numerator/normalization state, a current F*d accumulator, bounded input/output and arithmetic scratch. Metadata and initial-ID generation remain paid. If histories do not fit, this plan is not admitted; random paging is not silently substituted.

Building history retains the earlier O(L^2) replay arithmetic, approximately O(m*d*L^2+n*d*L^2) for m memberships, with repeated source passes. This is not the faster O(L) grouped recurrence. Rounding is nonlinear: in general, `sum round(x_i) != round(sum x_i)`. Consequently replacing the per-node replay by the old grouped linear recurrence is invalid without a new proof. Exact same-factor/full-state fixed-point propagation shares the raw error bound and may be much faster if n*d state fits; its larger state is the tradeoff.

With a declared nonzero norm floor mu and `A=sum |alpha_t|`, choosing

```text
eta = sqrt(d)*L*Delta/2
eta <= mu/4 and eta <= tau*mu/(8*A)
```

is sufficient for the ideal norm-based perturbation bound to leave at least half the tolerance for normalization/output arithmetic, assuming every contributing raw row has norm >=mu or an independently certified zero. It is NOT a completion condition for the more conservative floor/ceiling publication gate below. The independent review gives a legal K3 example satisfying these inequalities whose actual gate refuses. For A=0 output is exactly zero. This gives a candidate precision depending logarithmically on L, d, 1/tau, 1/mu and initialization scale, not on the LCM of graph degrees. A norm floor is a condition, not something graph size/counts alone establish. Only the actual output check authorizes publication; unsuccessful precision choices consume real retry work.

## Certified Normalized Publication

For one integer row e at layer t let `S=sum e_j^2`, `h=floor(sqrt(S))` and `c=ceil(sqrt(d))`. In lattice units the raw L2 error is at most `r=t*c/2`. If `h>r`, a safe normalized-vector error bound is

```text
beta_t = 2*r/(h-r) = 2*t*c/(2*h-t*c).
```

This is the earlier normalization perturbation inequality with the grid scale canceled. It is conservative; it is not valid when the denominator is nonpositive. A known isolate layer, exact initialization zero, or separately certified exact-zero route can use the zero vector. A computed integer zero alone supplies no such proof.

To enclose normalize(e) without floating sqrt, compute `z=floor(sqrt(S*2^(2p_s)))`. Then sqrt(S) lies between z/2^p_s and (z+1)/2^p_s. Integer divisions with outward rounding enclose each normalized coordinate on a dyadic output lattice 2^-p_o. Expand it by `ceil(beta_t*2^p_o)` lattice units. Multiply endpoints by dyadic alpha with sign-aware endpoint order, sum exactly on a common dyadic lattice, convert the midpoint to the requested finite format, and check its maximum distance from both exact dyadic endpoints against tau.

This avoids a second hidden LCM in normalization endpoint accumulation. The sqrt numerator needs roughly `2*bit_length(M)+log2(d)+2p_s` bits. Products/division scratch, alpha exponent span, final sums and float-conversion checking require explicit allowances. Dyadic output grids remove denominator multiplication across layers; they do not make arithmetic workspace free.

Any undecided normalization or output-format check prevents a success manifest. The job may retry at a pre-admitted larger b, take a separately certified exact-zero/other route, or fail explicitly. Retries and any discarded staged output are paid. Streaming a partial vector table is not a successful complete answer.

## Separating Scenarios And Strong Controls

1. **Denominator diversity:** disjoint cliques of prime degrees have few source features but a large common degree denominator. This route's feature-scalar width stays fixed once b is chosen. Compare against the existing *global* Q reservation, not an optimal component-local rational implementation. A clique-specialized eigenformula or component-local denominator solver is a required stronger control and can erase much of that comparison.
2. **Nonzero, well-conditioned output:** a constant first initialized coordinate remains one on nonisolates, providing a legitimate norm floor while other dimensions evolve nontrivially. The retained prime-degree fixture uses this to execute complete normalized output, not just raw layers. This explicit initialization is supported, but it does not prove the default SHA profile has the same floor.
3. **Near-zero failure:** a three-clique initialized to (0,1,1) in one dimension at Delta=1 gives the second node exact layer-one value 1/2 but rounded value zero. Its true normalized answer is one. Zero-on-rounding is wrong; the gate must reject. Finer admitted precision can rescue this particular fixture, not every possible near-zero input.
4. **Grouped shortcut failure:** two ties 1/2+1/2 round separately to zero while rounding their sum gives one. The storage/no-Q advantage must not be combined with the old grouped time bound without accounting for this nonlinearity.

## Primary Art And Candidate Boundary

[The completed independent review](Embeddings-Defect-Prior-Art.md) inspects six primary full texts. The closest elementary precedent is Nedic et al., [quantized distributed averaging](https://www.mit.edu/~jnt/Papers/J123-09-quant-averaging.pdf), whose Section V already compares grid-quantized and unquantized trajectories with linear-in-time error. Its consensus assumptions and round-down rule differ; its stability principle is not new here. The review also inspects [Higham and Knight's matrix powers](https://eprints.maths.manchester.ac.uk/345/1/0616025.pdf), validated dot-product bounds and defect/precision literature. It supports the core recurrence and publication certificate, qualifies the M=0 operand reservation and precision recipe, and supplies stronger same-factor/external-state controls. Five independent scalar checks passed; it did not rerun the lead implementation.

The local candidate is a whole-row normalized graph embedding schedule that simultaneously removes node layers/norm planes and degree-LCM-dependent scalar growth, with exact integer replay, a constructive width bound and explicit output failure conditions. A generic factor-aware fixed-point replayer can implement exactly this procedure. No stand-alone novelty follows from the elementary error bound or bit count. Useful-workload completion, same-factor timing/traffic, precision versus norm distribution, and inspected closest-art comparisons remain necessary for a paper-worthy claim.

## Retained New Probe

Python 3.11 standard library. Tiny input tables and dense exact/full-state comparators are verifier fixtures. The replay kernel holds feature histories and one row's layer gathers, not n-by-d state. This program does not implement the external builder, packed integer storage, process RSS cap or crash-safe output publication. Every accepted output row is converted to binary64 and checked against an independently propagated exact-rational reference enclosed at a finer normalization grid.

```python
from fractions import Fraction as F
from hashlib import sha256
from math import isqrt, lcm
from random import Random

def round_rational_integer_even(num, den):
    q, r = divmod(num, den)
    return q + int(2*r > den or (2*r == den and q % 2))

def build_incidence_operator_metadata(rows, weights):
    counts = [sum(f in row for row in rows) for f in range(len(weights))]
    assert all(t >= 2 for t in counts)
    q = [sum(weights[f] for f in row) for row in rows]
    degree = [sum(weights[f]*(counts[f]-1) for f in row) for row in rows]
    return q, degree

def replay_lattice_node_layers(i, history, initial, rows, weights, q, degree, depth):
    dim = len(initial[i])
    gathered = [[0]*dim for _ in range(depth)]
    for f in rows[i]:
        for t in range(depth):
            for j in range(dim):
                gathered[t][j] += weights[f]*history[t][f][j]
    value = initial[i][:]
    yield value
    for t in range(depth):
        value = ([round_rational_integer_even(gathered[t][j]-q[i]*value[j], degree[i])
                  for j in range(dim)] if degree[i] else [0]*dim)
        yield value

def build_lattice_feature_history(rows, weights, initial, depth, q, degree):
    if not rows:
        assert not initial and not weights and not q and not degree
        return []
    dim, history = len(initial[0]), []
    for t in range(depth):
        current = [[0]*dim for _ in weights]
        for i, row in enumerate(rows):
            value = None
            for value in replay_lattice_node_layers(i,history,initial,rows,weights,q,degree,t):
                pass
            for f in row:
                for j in range(dim):
                    current[f][j] += value[j]
        history.append(current)
    return history

def enclose_integer_normalized_vector(value, precision):
    square = sum(x*x for x in value)
    if not square:
        return [(F(0),F(0)) for _ in value]
    ps = precision+4
    lower = isqrt(square << (2*ps))
    upper = lower+1
    result = []
    for x in value:
        num = x << (precision+ps)
        den_lo, den_hi = (upper,lower) if x >= 0 else (lower,upper)
        lo = num//den_lo
        hi = -((-num)//den_hi)
        left,right = F(lo,1<<precision),F(hi,1<<precision)
        # Test-only exact inequalities independently certify the sqrt enclosure.
        if x >= 0:
            assert 0 <= left <= right and left*left*square <= x*x <= right*right*square
        else:
            assert left <= right <= 0 and right*right*square <= x*x <= left*left*square
        result.append((left,right))
    return result

def publish_lattice_embedding_row(layers, degree, alpha, tau, precision=40):
    dim, sums = len(layers[0]), [[F(0),F(0)] for _ in layers[0]]
    root_dim = isqrt(dim)
    root_dim += root_dim*root_dim < dim
    for t, (value, weight) in enumerate(zip(layers,alpha)):
        if not weight:
            continue
        known_zero = (t == 0 and not any(value)) or (t > 0 and degree == 0)
        if known_zero:
            bounds = [(F(0),F(0)) for _ in value]
        else:
            h = isqrt(sum(x*x for x in value))
            denominator = 2*h-t*root_dim
            if denominator <= 0:
                return None
            beta = F(2*t*root_dim, denominator)
            units = -((-beta.numerator*(1<<precision))//beta.denominator)
            margin = F(units,1<<precision)
            bounds = [(lo-margin,hi+margin) for lo,hi in
                      enclose_integer_normalized_vector(value,precision)]
        for j,(lo,hi) in enumerate(bounds):
            if weight < 0:
                lo,hi = hi,lo
            sums[j][0] += weight*lo
            sums[j][1] += weight*hi
    output = []
    for lo,hi in sums:
        value = float((lo+hi)/2)
        exact = F.from_float(value)
        if max(abs(exact-lo),abs(exact-hi)) > tau:
            return None
        output.append(value)
    return output

def validate_lattice_replay_fixture(rows, weights, initial, depth, b, alpha, tau):
    n, dim, scale = len(rows), len(initial[0]), 1<<b
    q, degree = build_incidence_operator_metadata(rows,weights)
    # Expanded matrix is constructed only here, in the independent test oracle.
    matrix = [[sum(weights[f] for f in set(row)&set(other)) if i != j else 0
               for j, other in enumerate(rows)] for i, row in enumerate(rows)]
    assert list(map(sum,matrix)) == degree
    history = build_lattice_feature_history(rows,weights,initial,depth,q,degree)
    replay = [list(replay_lattice_node_layers(i,history,initial,rows,weights,q,degree,depth))
              for i in range(n)]  # Test-only collection for oracle comparison.
    exact = [[[F(x,scale) for x in row] for row in initial]]
    rounded = [initial]
    maximum = max(abs(x) for row in initial for x in row)
    assert all(abs(x) <= n*maximum for layer in history for vector in layer for x in vector)
    for t in range(depth):
        exact.append([[sum(F(matrix[i][h],degree[i])*exact[-1][h][j] for h in range(n))
                       if degree[i] else F(0) for j in range(dim)] for i in range(n)])
        rounded.append([[round_rational_integer_even(sum(matrix[i][h]*rounded[-1][h][j]
                              for h in range(n)),degree[i]) if degree[i] else 0
                         for j in range(dim)] for i in range(n)])
    checked, accepted, rejected, complete = 0,0,0,True
    for i in range(n):
        for t,value in enumerate(replay[i]):
            assert value == rounded[t][i]
            for j,x in enumerate(value):
                assert abs(x) <= maximum
                assert abs(F(x,scale)-exact[t][i][j]) <= F(t,2*scale)
                checked += 1
        answer = publish_lattice_embedding_row(replay[i],degree[i],alpha,tau)
        if answer is None:
            rejected += 1; complete = False
            continue
        bounds = [[F(0),F(0)] for _ in range(dim)]
        for t,weight in enumerate(alpha):
            denominator = lcm(*(x.denominator for x in exact[t][i]))
            integers = [int(x*denominator) for x in exact[t][i]]
            for j,(lo,hi) in enumerate(enclose_integer_normalized_vector(integers,80)):
                if weight < 0: lo,hi=hi,lo
                bounds[j][0] += weight*lo
                bounds[j][1] += weight*hi
        for y,(lo,hi) in zip(answer,bounds):
            assert max(abs(F.from_float(y)-lo),abs(F.from_float(y)-hi)) <= tau
        accepted += 1
    return checked,accepted,rejected,complete,history

assert [round_rational_integer_even(n,2) for n in (-5,-3,-1,1,3,5)] == [-2,-2,0,0,2,2]
assert round_rational_integer_even(1,2)*2 != round_rational_integer_even(2,2)
assert build_lattice_feature_history([],[],[],4,[],[]) == []
zero_depth = validate_lattice_replay_fixture([(),()],[],[[8,0],[0,0]],0,3,[F(1)],F(1,1000))
assert zero_depth[3] and zero_depth[4] == []
rng, totals = Random(733), [0,0,0,0]
for case in range(72):
    n, dim, depth, b = rng.randrange(3,10), 3, rng.randrange(2,7), 14
    feature_sets = []
    for _ in range(rng.randrange(1,7)):
        members = {i for i in range(n) if rng.randrange(2)}
        if len(members)>=2: feature_sets.append(members)
    rows = [tuple(f for f,members in enumerate(feature_sets) if i in members) for i in range(n)]
    weights = [rng.randrange(1,8) for _ in feature_sets]
    initial = [[(1 if j==0 else rng.randrange(-1,2))*(1<<b) for j in range(dim)] for _ in rows]
    alpha = [F(1,2) if t%2 else F(-1,4) for t in range(depth+1)]
    result = validate_lattice_replay_fixture(rows,weights,initial,depth,b,alpha,F(1,100))
    for j in range(3): totals[j] += result[j]
    totals[3] += result[3]
print('random_lattice',dict(fixtures=72,coordinate_checks=totals[0],accepted_rows=totals[1],
                           rejected_rows=totals[2],complete_jobs=totals[3]))

sha_counts = [0,0,0,0]
for seed in range(8):
    n,dim,depth,b = 9+seed%3,8,4,22
    sets = [{i for i in range(n) if (i+f)%4 != 0} for f in range(4)]
    rows = [tuple(f for f,members in enumerate(sets) if i in members) for i in range(n)]
    initial = []
    for i in range(n):
        vector = []
        for j in range(dim):
            digest = sha256(b'DFHR-v1'+seed.to_bytes(32,'little')+
                            i.to_bytes(8,'little')+j.to_bytes(4,'little')).digest()
            u = int.from_bytes(digest[:8],'little')
            z = 0 if u & 1 else (-1 if (u >> 1) & 1 else 1)
            vector.append(z*(1<<b))
        initial.append(vector)
    result = validate_lattice_replay_fixture(rows,[1,2,3,5],initial,depth,b,
                                           [F(1,4)]*(depth+1),F(1,1000))
    for j in range(3): sha_counts[j] += result[j]
    sha_counts[3] += result[3]
print('sha_profile',dict(fixtures=8,sparsity_bits=1,dimension=8,depth=4,
      coordinate_checks=sha_counts[0],certified_rows=sha_counts[1],
      refused_rows=sha_counts[2],complete_jobs=sha_counts[3]))

primes = [3,5,7,11,13,17,19,23]
rows = [(f,) for f,p in enumerate(primes) for _ in range(p+1)]
b, depth, dim = 24,24,3
initial = [[(1<<b),(1<<b)*((i%3)-1),(1<<b)*((i%5)-2)//2] for i in range(len(rows))]
alpha = [F(0)]*depth+[F(1)]
result = validate_lattice_replay_fixture(rows,[1]*len(primes),initial,depth,b,alpha,F(1,100000))
assert result[3]
common = lcm(*primes)
old_width = 2+(3*len(rows)*common**depth).bit_length()
history_width = 2+(len(rows)*(1<<b)).bit_length()
numerator_width = 2+(3*max(primes)*(1<<b)).bit_length()
print('prime_degree',dict(nodes=len(rows),features=len(primes),depth=depth,
      exact_global_Q_bits=common.bit_length(),old_reserved_width=old_width,
      lattice_history_width=history_width,lattice_numerator_width=numerator_width,
      accepted_rows=result[1],coordinate_checks=result[0]))

rows, weights = [(0,),(0,),(0,)], [1]
bad = validate_lattice_replay_fixture(rows,weights,[[0],[1],[1]],1,0,[F(0),F(1)],F(1,1000))
assert bad[2] == 3 and not bad[3]
rescued = validate_lattice_replay_fixture(rows,weights,[[0],[1<<18],[1<<18]],1,18,
                                       [F(0),F(1)],F(1,1000))
assert rescued[3]
print('boundaries negative_ties=passed; grouped_rounding=refuted; rounded_zero=refused;'
      ' admitted_finer_nonzero_fixture=complete; empty_and_depth_zero=passed')
```

## Verification Receipt

The first execution exited 0: 72 randomized fixtures, 6,723 raw-coordinate checks, 448 accepted rows and 72 complete jobs. The prime-degree case checked 7,950 coordinates and returned all 106 rows at tolerance 1e-5; its stated widths were 652 bits for the older global-Q reservation and 33 bits for both lattice history and weighted numerator. These randomized fixtures deliberately include a constant nonzero initialized coordinate and therefore are not an unbiased eligibility sample.

Subsequent development corrected the rescue example to keep the same real initialization at both precisions, and moved dense matrix construction from a shared metadata helper into the independent oracle only. The replay builder now receives fixture metadata rather than allocating the expanded matrix. Both revised runs exited 0 with the same main receipts; the low-precision case rejected all three rows and the finer version completed. Adding exact squared inequalities independently checking every normalizer enclosure also passed with the same counts. These are corrections to experimental isolation and coverage, not evidence of a physical-memory cap. Near-zero refusals and numeric-width comparisons remain separate from successful complete-output fixtures and measured RAM.

The SHA-profile extension uses the declared `DFHR-v1` initialization with sparsity parameter r=1, eight seeds, dimension eight and four layers, without adding a constant coordinate. Its execution exited 0: 3,160 raw-coordinate checks, 79 certified rows, no refused rows and eight complete jobs at tolerance 1e-3. These are eight constructed overlap graphs, not empirical acceptance probabilities for real workloads or all seeds. The explicit near-zero refusal remains a distinct counterexample.

Final boundary execution also exited 0 after adding an empty-universe preallocation bypass and depth-zero output checks, including an exactly zero initialized row. All preceding counts remained unchanged. No local test process remains running at this checkpoint. Independent numerical/prior-art review remains separate from these lead executions.

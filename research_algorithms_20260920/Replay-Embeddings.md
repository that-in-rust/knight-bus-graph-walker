# Replay Embeddings Through Diagonal-Feature History

Date: 2026-09-20. Family A06. Proposed algorithm: **Diagonal Feature History Replay**. This is an exact-real-arithmetic state-elimination result with a qualified, explicitly specified numerical publication contract. It is not a production FastRP implementation, universal 4 GB solution, or established global novelty claim.

## Premise Check

**Proposed delta:** eliminate every node-by-dimension intermediate layer *and every node-by-layer norm array*. Store a short history of feature-by-dimension aggregates. Reconstruct one node's complete layer sequence locally, normalize its vectors using all dimensions, and emit its final vector in row-major order. This differs materially from dimension-striped norm accumulation followed by a second propagation replay.

This works for a precisely declared incidence-derived operator, not an arbitrary graph that is merely compressible. [D14](../research_4gb_20260919/02-prd06-Architectures.md#h4-norm-separated-fastrp-replay) already supplies dimension-striped replay; [D06/D16](../research_4gb_20260919/Architecture-Decision-Map.md) supply incidence operators and stationary feature-state elimination. Those are inherited ideas. The [final decision brief](../research_4gb_20260919/Final-Research-Decision-Brief.md) requires preparation, query, complete output and refresh to count. This document does not modify the PageRank proposal.

The important retained difficulty is **normalization near exact zero**. The independent review exposed a legal default-seed failure. The revision below adds an exact group-zero certificate and an integer-scaled grouped completion domain. These resolve that fixture without blind precision retries and remove quadratic history-building arithmetic when the diagonal has few distinct values. They do not guarantee that arbitrary feature counts, denominators or output tolerances fit. The original interval route remains conditional; a declined publication is a failed job.

## Expert Lenses And Alternatives

Numerical linear algebra asks whether the factorization and cancellation are exact and stable. External-memory analysis asks whether state was removed or merely renamed. Graph semantics asks whether the input really specifies weighted shared-feature edges. The adversarial lens attacks singleton groups, node-specific randomness, near-zero vectors, large feature counts and refresh fanout. These are self-review lenses, not independent experimental confirmation.

Candidate alternatives considered:

1. **Dimension-striped replay:** retain D14 as the general linear-operator baseline. It needs node-sized norm state or charged norm spills and can require output transposition.
2. **Norms from propagated scalar second moments:** rejected. Neighbor vectors become correlated; their individual norms do not determine the norm of their sum. A full covariance/Gram state can become quadratic.
3. **Pure feature quotient `P=UV^T`:** useful when exactly valid, but fails for a self-excluded shared-feature projection and distinct random node vectors.
4. **Diagonal plus feature history, selected:** retain the local diagonal feedback explicitly, aggregate only the shared part, and reconstruct actual full vectors before normalization. Small propagation depth is essential to its time/space tradeoff.

## Primary Paper Comparison

The [evidence ledger](similarity-embeddings-Evidence.md) records what was inspected, not just which titles were found.

| Primary source | Established result or implementation | Consequence here |
| --- | --- | --- |
| [Chen et al., FastRP, CIKM 2019](https://arxiv.org/pdf/1908.11512), sections 3.1-3.5, Algorithm 1 | Sparse random projection of a weighted sum of powers, a right-side degree scaling matrix, and associative computation without materializing graph powers | Random projection, linear propagation, weighted layer combination and avoiding `P^l` are old. The displayed paper algorithm is not itself the GDS row-normalized combination contract. |
| [Authors' `fastrp.py`](https://github.com/GTmac/FastRP/blob/master/fastrp.py), `fastrp_projection` and `fastrp_merge` | Selectable adjacency/transition inputs, optional degree scaling, optional per-layer row normalization, and final feature standardization through `scale` | Even the authors' code is not simply the displayed paper equation. We explicitly omit final column standardization; matching it would need additional charged passes and a variance/zero-variance contract. |
| [GDS FastRP reference](https://neo4j.com/docs/graph-data-science/current/machine-learning/node-embeddings/fastrp/), introduction/operator equations | Neighborhood averaging of raw intermediate vectors, then weighted row-normalized vectors, plus extensions including node properties and node self influence | Motivates our normalized output mode, but does not prove RNG, arithmetic or source-level GDS parity. No such parity is claimed. |
| [Li, Hastie, Church, Very Sparse Random Projections, KDD 2006](https://hastie.su.domains/Papers/Ping/KDD06_rp.pdf), sections 1 and 4.1, Lemmas 1-3 | Sparse signed projections and exact variance expressions, including fourth-moment terms | Sparsity and generic distance preservation are not new. Preserving this particular initialized embedding is distinct from proving downstream quality or a Johnson-Lindenstrauss bound. |
| [Zhou, Huang, Scholkopf, Learning with Hypergraphs, NeurIPS 2006](https://papers.nips.cc/paper_files/paper/2006/file/dff8e9c2ac33381546d96deea9922999-Paper.pdf), sections 3-6, equation 3 | Incidence-based transition `D_v^-1 H W D_e^-1 H^T`, including a feature-size factor and self-transition behavior | Incidence-native embedding operators predate this work. Their normalization is not silently interchangeable with our self-excluded shared-count graph. |
| [Gu, Goel, Re, S4](https://arxiv.org/pdf/2111.00396), sections 2.4 and 3, Theorems 2-3 | Diagonal-plus-low-rank state-space representations, recurrence and generating-function/Cauchy-kernel evaluation | Strong prior-art warning: diagonal-low-rank algebra and time/space transformations are established. Candidate difference is finite-depth graph projection, original-node reconstruction, exact row normalization and a feature-sized physical schedule. |

The possible contribution is that particular elimination/normalization schedule and its bounded-resource crossover, not a new random projection distribution, generic low-rank factorization, SpGEMM, or checkpointing principle. A broader search of structured matrix-polynomial evaluation and factorized graph embeddings remains necessary.

The reviewed [low-rank matrix-function paper, Theorem 3.2](https://arxiv.org/pdf/1707.03045), establishes finite polynomial exactness with Krylov representations. [LSSL, Theorem 2](https://papers.neurips.cc/paper_files/paper/2021/file/05546b0e38ab9175cd905eebcc6ebb76-Paper.pdf), explicitly separates exact arithmetic operation counts from bit complexity and stability. Both were inspected in this revision. Our grouped closure is also an elementary structured recurrence, not a new algebraic theorem. The remaining question is the graph-specific state, completion and full-output schedule under a measured budget.

[RandNE, Algorithms 1 and 3 / section IV-D](https://zw-zhang.github.io/files/2018_ICDM_RandNE.pdf), was also inspected. It already propagates random projected vectors iteratively and updates retained intermediate projections under graph changes. Its displayed output is a linear combination with orthogonal Gaussian initialization, not our per-layer normalized SHA profile. Compare its retained node-layer state and incremental update work against our reconstruction and full refresh; do not claim dynamic projection updates or dimension independence as new, or assume its raw-coordinate identity certifies our normalized floating output.

## Exact Operator Contract

### Input And Projection

Let `B` be a binary `n x F` node-feature incidence matrix. The raw source retains all memberships, including singleton features. Repeated `(node,feature)` records are deduplicated. Feature IDs with identical memberships remain distinct if the input semantics assign separate additive weight to them.

Each feature has an integer weight `w_f` in `[1,2^32-1]`; the default is one. Let `t_f=sum_i B_if`. The analytical matrix suppresses features with `t_f<2`, without discarding their raw refresh provenance. Subsequent definitions of `B,w,F` refer to active features. Every node, including isolates, remains in the universe.

Define

```text
q_i = sum_f w_f B_if
s_i = sum_f w_f B_if (t_f-1)
W_ij = sum_f w_f B_if B_jf  for i != j; W_ii=0
P_ij = W_ij/s_i if s_i>0; otherwise 0.
```

This is a loop-free, undirected weighted graph whose propagation is a *row-stochastic outgoing-neighbor average* on nonisolates. Isolate rows are zero, not identity and not redistributed. Overlapping shared features add their weights; replacing `W_ij` by `1[W_ij>0]` is unsupported. Directed graphs, arbitrary pair predicates and negative relationship weights are excluded from this initial profile. Counts and weighted sums are exact checked integers, admitted only when `q_i,s_i,t_f` fit their declared widths.

Write

```text
d_i = -q_i/s_i when s_i>0, else 0
D = diag(d_i)
U_if = B_if*w_f/s_i when s_i>0, else 0
V_if = B_if
P = D + U V^T.                                      (1)
```

Since every active feature has at least two members, `q_i<=s_i` and `-1<=d_i<=0`. Equation (1) exactly cancels self contributions; there is no assumption that `P` itself has low rank. Both `U` and `V` are accessed through the incidence records; neither dense `n x F` factor is stored.

### Initialization, Layers And Output

Choose dimension `1<=d<=2^32`, depth `L>=0`, exact finite dyadic layer weights `alpha_0,...,alpha_L`, and a reproducible initialization `R`. The default has no degree scaling (`g_i=1`). Optional node scaling consists of an explicit finite nonnegative binary64 input `g_i`, interpreted as an exact dyadic value and charged as an input/prepared column. There is no implicit `pow(degree,beta)` convention. A paper-style degree multiplier must be supplied and identified explicitly, not inferred from a parameter name. Checked size arithmetic and the RAM gate further restrict feasible dimensions and depth.

One fully defined initialization profile is:

```text
hash = SHA-256(ASCII("DFHR-v1") || seed_32_bytes || LE64(stable_node_id)
               || LE32(dimension_index))
u = LE64(first 8 digest bytes)
z_ij = 0 unless the lowest r bits of u are zero
z_ij = +1 or -1 according to bit r when nonzero; r in [0,32]
R_ij = g_i * z_ij.
```

Take sign-bit value zero as positive. When `r=0`, the empty low-bit test always passes. Stable numeric IDs are part of the dataset manifest, not rebuilt arbitrary dense ranks. A uniform positive projection scale is omitted because it cancels under each row normalization; it does **not** cancel in the optional unnormalized output profile. This defines deterministic pseudorandom inputs, not an assertion that SHA-256 outputs over a fixed seed satisfy every independent-random-matrix theorem. Statistical embedding quality remains separate evidence.

The requested mathematical result is

```text
E_0 = R
E_(l+1) = P E_l
nu(x) = x/||x||_2 when ||x||_2>0; otherwise the zero vector
Y_i = sum_(l=0..L) alpha_l * nu(E_l[i,:]).              (2)
```

Normalization occurs only in output combination, **not inside propagation**. There is no final unit normalization and no final column centering/scaling. A separate raw-linear mode replaces `nu(x)` with `x` and retains the initialization scale. A source-provided small explicit `R` is also a valid oracle fixture. Node-property projection, learned dimension mixing, GraphSAGE and node2vec training are not inherited capabilities.

The output is one full vector per original node, in original-ID order, with generation/operator/seed identifiers. An empty node universe publishes an empty result without allocating histories. The final format is caller-selected binary32 or binary64 plus an absolute coordinate-error tolerance `tau>0`; a query succeeds only when **every coordinate of every returned row** is certified within `tau` of (2). It does not promise bitwise equality to another reduction schedule.

## Elimination Mechanism

Define the feature history

```text
Z_l = V^T E_l,                    shape F x d.
```

For a fixed row `i`, equation (1) gives the local recurrence

```text
e_0 = R_i
e_(j+1) = d_i e_j + U_i Z_j.                          (3)
```

Unrolling yields

```text
E_l[i,:] = d_i^l R_i
           + sum_(j=0..l-1) d_i^(l-1-j) U_i Z_j.       (4)
```

Thus after computing `Z_0,...,Z_(L-1)`, reconstruct *all* node layers from `O(L*d)` local scratch and these `L*F*d` history values. A separate `n*(L+1)` norm plane is unnecessary: each reconstructed row has all its dimensions present when its norm is evaluated.

To build `Z_t`, stream every original node, reconstruct `E_t[i,:]` from the previously completed history, and scatter it through `V_i` into the one new feature accumulator. Once the pass finishes, freeze that accumulator as history. For output, stream rows once more; compute (3), normalize each layer immediately, and accumulate its weighted contribution to `Y_i`.

### Why This Is Not A Hidden Gram Or Expression Matrix

The generating-function identity is

```text
Z(z) = V^T (I-zD)^-1 R + z V^T (I-zD)^-1 U Z(z).
```

Expanding the middle term would introduce moments `V^T D^j U`, each potentially dense `F x F`. **The proposed algorithm never forms them.** It also never forms `R R^T`, an all-node expression DAG, Krylov basis with `n*d` rows, `n*L` norms or cross-layer `F^2*L^2` Gram matrices. The scan and local recurrence implement the identity instead. Repeated arithmetic replaces persistent node-layer state.

### Real-Arithmetic Proof

**Theorem.** Exact execution of (3) and streamed feature aggregation produces the same raw layers, row norms and output (2) as full-matrix propagation on the declared graph.

**Proof.** Initially `Z_0=V^T R`. Assume all histories through `Z_(t-1)` are exact. Starting from `R_i`, recurrence (3) is exactly the `i`th row of `E_(j+1)=D E_j+U Z_j`, so induction on `j` gives the exact `E_t[i,:]`. Summing these reconstructed rows with `V_if` gives the exact `Z_t`. This proves the history induction. Applying the same recurrence in the output pass reconstructs each full row and therefore its exact norm. The definition of `nu`, including its zero rule, then gives (2). QED.

No source-row equality, rank approximation, or probabilistic assumption enters the proof. In particular, node-specific randomness remains in the `d_i^l R_i` term.

## Pseudocode And High-Degree Rows

```text
build_feature_history_replay(incidence, initialization, depth):
    history = admitted storage for depth feature-by-dimension arrays
    for t in 0 .. depth-1:
        Z_t = zeroed feature accumulator, frozen only after entire pass
        for node i in stable ID order:
            R_i = regenerate initialization row
            g[0..t-1,:] = 0                 # at most L*d, not degree*d
            scan incidence row i:
                for j in 0 .. t-1:
                    g[j,:] += U_if * history[j][f,:]
            e = R_i
            for j in 0 .. t-1: e = d_i*e + g[j,:]
            rescan incidence row i:
                Z_t[f,:] += V_if*e
        freeze Z_t in history
    return history

emit_normalized_embedding_rows(history, initialization, weights):
    for node i in stable ID order:
        gather g[0..L-1,:] by one incidence-row scan
        e = regenerate initialization row; y = alpha_0*nu(e)
        for j in 0 .. L-1:
            e = d_i*e + g[j,:]
            y += alpha_(j+1)*nu(e)
        certify_and_stage_complete_row(i, y)
    publish complete output only after every row and checksum succeeds
```

At `t=0` the gather scan is unnecessary. If a row does not fit a small decode buffer, the scatter phase reopens its bounded cursor rather than retaining its feature IDs. The `g` slab combines *all histories* while visiting a membership, eliminating a feature-list-sized scratch allocation. On the admitted path feature histories are resident; if they do not fit, a different explicitly costed plan is required. Unbounded random paging is not an implementation of the stated RAM/I/O claim.

For `L=0`, emit initialized/normalized rows with no history or topology propagation. A zero layer weight can skip its output normalization, but its raw layer may still be needed for subsequent propagation.

## Numerical Contract And Guarantee

### Certified Interval Profile

Use outward-rounded interval arithmetic for every aggregate, coefficient division, multiplication, addition, square root and output combination. Start from exact integer counts and dyadic inputs. An interval scalar occupies `p=16` bytes for two binary64 endpoints; the implementation must disable unsafe reassociation and verify directed rounding behavior. A wider-precision profile has a separately charged `p`, including its numerical workspace. This is a proposed implementation obligation, not a library certification already performed.

An exact primitive's range must be enclosed. Nonfinite endpoints, overflow and unsafe division are certification failures, not values silently clamped to zero. From the local coordinate intervals, compute an outward norm interval

```text
n_low  = sqrt_down(sum_down(square_down(min_abs(interval_j))))
n_high = sqrt_up  (sum_up  (square_up  (max_abs(interval_j)))).
```

If `n_low>0`, interval division encloses the normalized row. If all coordinates are singleton zero intervals, use the zero vector. If `n_low=0` but the row is not certified zero, report **undecided normalization**. Never infer a zero vector merely from a rounded zero norm.

After weighted combination, let the exact output coordinate lie in `[lo,hi]`. Choose a finite output float `y_hat` and accept it only when an outward check gives `max(|y_hat-lo|,|hi-y_hat|)<=tau`. This includes final binary32/binary64 conversion error. Accepted coordinates therefore satisfy `|y_hat-Y_ij|<=tau`, and an accepted row has L2 error at most `sqrt(d)*tau`.

**Guarantee.** Induction over interval operations encloses every exact value used in the real-arithmetic proof. The normalization lower bound excludes a singular denominator. The final containment check consequently establishes the stated error for every published complete result. This is a conditional successful-output theorem, not a proof that all admissible graph inputs will pass at binary64 precision.

For an approximate vector `x_hat` with certified L2 error `eta<||x_hat||`, a useful additional diagnostic bound is

```text
||nu(x_hat)-nu(x)||_2 <= 2*eta/(||x_hat||_2-eta).
```

It follows by inserting `x_hat/||x||` and using the reverse triangle inequality. This exposes conditioning; it does not justify replacing the interval test by an unchecked relative tolerance.

### Failure, Retry And Exactness Boundary

The certified schedule may retry from initialization at a pre-admitted wider precision. Every retry charges complete additional scans, history bytes and staged-output replacement. Precision escalation is capped. A certified result cannot be promised for arbitrary near-zero rows at a fixed finite precision.

Exact rational recomputation can decide raw zero but may require large integers. The revision below supplies **two explicitly bounded completion domains**, replacing the earlier absence of an exact-zero route: zero grouped initial moments, and admitted integer-scaled grouped propagation. Neither makes all rational graph inputs cheap. Outside them, a separately admitted direct-operator or wider interval plan may run; D14 alone does not decide exact zero. If no admitted plan certifies every row, terminate without a successful output manifest and count a failed completion.

The sparse projection's statistical distortion, downstream classification/recall and exact top-k on embedding vectors are separate contracts. The source paper's probabilistic guarantees do not turn this custom initialization or its rounded normalized outputs into exact graph-similarity answers.

## Revision: Diagonal-Grouped Completion

### Closed Moments Without A Dense Operator

Partition **active rows** by exactly equal rational `d_i`, using reduced integer numerator/denominator comparisons, never rounded floating equality. Let there be `G` values `d_g` and diagonal masks `I_g`. Isolates need no moment group because their incidence rows vanish. Define

```text
Z_(g,t) = V^T I_g E_t,             shape F x d for each g
H_t     = sum_g Z_(g,t) = V^T E_t
Z_(g,t+1) = d_g Z_(g,t) + V^T I_g U H_t.             (5)
```

**Proof.** Multiply `E_(t+1)=D E_t+UV^T E_t` by `V^T I_g`; use `I_g D=d_g I_g`. Summing groups recovers `H_t`, since omitted isolate rows have zero `V`. Induction yields the same histories as (3). Equation (5) is standard invariant-moment closure. No independent algebraic novelty is asserted.

The operator action `V^T I_g U H_t` is computed by a streamed row gather and a second cursor scan to scatter into that row's group. There is no `F x F`, `GF x GF`, `n x d` or expression matrix. Store `H_0,...,H_(L-1)` for full-row output reconstruction as before; storing only current group moments is **insufficient** for that output contract.

```text
build_grouped_feature_history(incidence, G, L):
    Z[g,f,:] = exact or enclosed sum of initial rows in group g containing f
    freeze H_0 = sum_g Z[g,:,:]
    for t in 0 .. L-2:
        multiply each Z[g,:,:] in place by d_g
        for original node i:
            v = gather U_i * frozen H_t with a bounded incidence cursor
            reopen that row; scatter v to Z[group_i,f,:] for its memberships
        freeze H_(t+1) = sum_g Z[g,:,:]
    release current grouped Z
    reconstruct and normalize original rows from frozen H, as in (3)
```

Only **one** mutable `G*F*d` plane is needed: once `H_t` is frozen, old groups are read only by their own diagonal scaling. Subsequent gathers use `H_t`, not partially overwritten groups. Checkpoint/recovery must redo a whole interrupted group update from the last complete group checkpoint; it cannot resume from a half-overwritten group plane. Persisting only `H_t` is insufficient to restart grouped updates when `G>1`.

### Exact Dependency-Aware Zero Route

Accumulate `Z_(g,0)` exactly as scaled integers from the dyadic initialization. If **every cell of every group is zero**, (5) preserves all groups as exact zero at every depth. Then

```text
E_t[i,:] = d_i^t R_i
nu(E_t[i,:]) = (-1)^t nu(R_i)   on active rows
nu(E_t[i,:]) = 0               on isolates when t>0.
```

Here all active `d_i<0`; positive scalar magnitudes cancel analytically, without computing tiny powers. Thus an active row's output is `(sum_t alpha_t*(-1)^t)*nu(R_i)`, and an isolate's output is `alpha_0*nu(R_i)`. This is an exact dependency certificate, not a threshold on a small interval. It needs one initialization membership scatter and a streamed output pass; no propagation scans, history, common transition denominator or finite-precision cancellation are needed. Initial group storage is explicitly charged at its exact accumulator width. A failed zero test continues to an independently admitted general schedule; its discovery work is not free.

For reviewer seed 8 on K4, `G=F=d=1`, `R=(1,-1,0,0)` and exact `Z_(1,0)=0`. The route publishes `(1,-1,0,0)` for `alpha=(0,0,1)` in either output format. The old independently enclosed per-node `(-1/3)R_i` terms created nonzero interval width; retaining their common dependence removes that failure. Even an interval implementation of (5) preserves a singleton-zero group, because interval multiplication by exactly zero remains zero, but the initial sum must first be proven exact.

**Counterexample to a weaker certificate.** Rows `{1},{1},{0},{0,1}`, unit feature weights and scalar `R=(-1,0,-1,1)` give `d=(-1/2,-1/2,-1,-2/3)`. The total `H_0=(0,0)`, but `H_1=(1/3,-1/6)`. Group moments are nonzero and cancel across different diagonal values. Checking only total `H_0` would publish the wrong result. The executed grouped probe rejects that shortcut.

### Integer-Scaled Completion Domain

For nonzero grouped moments, let `S=2^e` clear all finite dyadic initialization denominators, so `X_0=S R` is integral; `M=max_ij |X_0ij|`. Let

```text
h_i = gcd { w_f : B_if=1 } on each active row
r_i = s_i/h_i
Q = lcm { r_i : s_i>0 }, with empty lcm=1
a_g = Q*d_g                         (integer)
b_if = (Q//r_i)*(w_f//h_i) when B_if=1
X_t = S*Q^t E_t
K_(g,t) = V^T I_g X_t;  J_t = sum_g K_(g,t)
K_(g,t+1) = a_g K_(g,t) + V^T I_g b J_t.             (6)
```

Use (6) with checked integers, the same in-place schedule, and reconstruct `x_(t+1)=a_group*x_t + b_i J_t`. All raw zeros are now decidable by integer comparison, including cancellation not captured by the group-zero certificate. `S*Q^t` is positive and cancels in each nonzero row normalization: compute `nu(x_t)` directly. There is no rational denominator per feature cell.

This reduced Q incorporates the follow-up review's improvement over the initial `lcm(s_i)` choice. The row gcd divides both q_i and s_i. It is sufficient because each coefficient can be formed by the displayed divide-before-multiply expression; it is necessary because an integer linear combination of the weights equals h_i, so any denominator clearing every w_f/s_i must be divisible by s_i/h_i. Thus Q is the smallest global positive integer clearing this U, and clears D too. Compute `a_i=-(Q//r_i)*(q_i//h_i)` similarly. A literal `Q*w_f/s_i` can create a much wider temporary than its final coefficient. Sixteen disjoint prime-weighted pairs need Q=1, not the product of their weights; the former 528-bit scalar allowance becomes nine bits in the review's L=8, M=1 example. This is removal of avoidable arithmetic inflation, not a new graph theorem.

This profile supports exactly the same graph, seed, node-specific scale, layer weights, isolates and normalization placement as (2). It is an **execution-domain restriction**, not a changed operator. Few distinct `d_i` do not imply small `Q`: even with constant `d`, row degree denominators in `U` can vary. Very large `Q`, initial scales or depth may defeat admission.

An adversarial family is disjoint stars encoded by size-two features. Every active row has `d_i=-1`, hence `G=1`, but centers with distinct prime degrees require `Q` divisible by their product. Our globally cleared denominator can be much larger than the denominators actually needed by individual rows at a given depth. That conservatism is a real admission loss; no uncharged local rational/expression system is assumed to repair it.

#### Width Bound

Since the exact `P` is substochastic with nonnegative entries, `|X_tij| <= M Q^t`. Each feature/group sum has magnitude at most `n M Q^t`. In split accumulation, row absolute coefficient sum is at most `3Q`: `|a_i|<=Q` and `sum_f b_if*t_f<=2Q`. Any partial scatter or diagonal-plus-scatter group value is consequently bounded by `3 n M Q^(t+1)`. A sufficient signed width through depth `L` is

```text
W = 2 + bit_length(max(1, 3*n*M*Q^L)).                (7)
p_int = rounded_limb_bytes(ceil(W/8)).
```

Products must use a checked workspace that can hold their result, not silently wrap in a machine word. The bound also covers membership-first local gathers and row recurrence intermediates; exact final values usually require fewer bits. `Q`, `S`, group rationals, alpha inputs, norm squares and division/square-root workspace are separate named integers. Compute a capped LCM by gcd/division before multiplication; reject before allocating an oversized product. Neither an `n`-entry denominator array nor arbitrary growing per-cell `Fraction` objects are part of the fixed-width schedule.

Evaluate (7) by capped products as well: starting at `max(1,3*n*M)`, reject whenever the next multiplication by `Q` would exceed the maximum admitted integer, using division to check first. Do not allocate an unbounded `Q^L` merely to discover its size. The revised executable probe uses these pre-multiplication guards. Initial exact group sums need only `W_initial=2+bit_length(max(1,n*M))`, and the common initialization scale itself has a separately admitted exponent/representation.

#### A Finite Output-Completion Guarantee

For an integer row `x`, determine `N=sum_j x_j^2` exactly. `N=0` returns zero. Otherwise, for a pre-admitted integer precision `b>=1`, calculate

```text
m = floor(sqrt(N*2^(2b))) using integer square root
sqrt(N) in [m/2^b, (m+1)/2^b],
    with equal endpoints m/2^b when m^2=N*2^(2b).
```

Divide integer coordinates by these positive rational endpoints with the appropriate sign. The lower endpoint is at least max(1,|x_j|), since |x_j| is an integer no larger than sqrt(N). Each coordinate interval therefore lies in [-1,1] and has width at most `2^-b`. Let `A=sum_t |alpha_t|`. Accumulate signed alpha-weighted rational intervals and intersect with `[-A,A]`. Choose `b` once so `A*2^-b<=tau/4`; `A=0` returns zero without propagation. Each final coordinate interval then has width at most `tau/4`. This tightens the earlier safe bound by one bit.

For a selected IEEE format with precision `p` bits and minimum subnormal `mu`, a conservative absolute nearest-rounding bound on finite `[-A,A]` is `rho(A)=2^(-p)*A+mu/2`, provided `A` is within the format's finite range. **Sufficient completion domain:** all integer/workspace/history bounds fit, `tau>=2*rho(A)`, and the specified correctly rounded conversion is available. Round the exact interval midpoint to the nearest output float. Its distance from the true coordinate is at most `tau/8+rho(A)<tau`. Therefore every row completes at this predetermined precision, including raw exact zeros; no propagation restart or blind precision retry is needed. The condition is sufficient, not necessary: the seed-8 exact integer outputs succeed even at much smaller tau.

Workspace is not free. A norm square needs at most `2W+ceil(log2(max(1,d)))` bits, and its shifted integer-square-root operand adds `2b` bits. A conservative rational-output endpoint denominator budget for a row is

```text
B_y = (L+1)*(W+b+ceil(log2(max(1,d)))+3) + E_alpha,
```

where `2^E_alpha` clears alpha denominators. Keep that common alpha denominator factored once, or use canonical reduced rationals; unreduced repeated addition must not silently duplicate it. Numerators add the bit length needed for `max(1,A)` and a sign. Output accumulators use `2*d` such rational endpoints, plus bounded multiplication/gcd/division scratch. Charge their **actual** packed sizes or reject. Rational endpoint workspace can dominate for large `L`; its use in the tiny probe is not a claim that Python `Fraction` objects occupy the packed bound.

Endpoint bounds are not complete temporary bounds. Generic midpoint formation from two endpoints whose denominator sizes are at most B needs up to `2B+1` denominator bits, corresponding numerator cross-products, and gcd workspace. Exact differences against an output float can additionally involve denominator `2^1074` for binary64 or `2^149` for binary32. Either reserve those products explicitly or specify an alternative bounded conversion/check primitive. The follow-up review constructed a valid case with 129-bit endpoint allowance but a 216-bit midpoint denominator. A production admission implementation must reject before any such workspace exceeds its reservation; the small Fraction-based probe does not enforce complete packed output-workspace admission.

There is also an unavoidable output-format obstruction. `L=0,d=9,alpha_0=1,R=(1,...,1)` requires coordinates `1/3`. Nearest binary32 error is `1/100663296`; binary64 error is `1/54043195528445952`. Both exceed `1e-20`, even with exact internal arithmetic. The executed test rejects that request as **output-format-impossible**, not zero-ambiguous or precision-exhausted. General non-singleton intervals need a nearest-format-cell exclusion proof to establish impossibility; otherwise report unresolved output enclosure. Also distinguish integer/history admission, finite-range overflow, and invalid snapshot failures.

### RAM And Work Delta

For `L>=2`, the generic grouped build reserves

```text
M_group_build <= p*(G+L)*F*d + O(p*d) + group/feature metadata
                 + bounded file/rounding/runtime/residency buffers.
M_group_output <= p*L*F*d + p*d*(L+3) + output-rational workspace
                  + the same applicable buffers.
M_peak = max(M_group_build, M_group_output, preparation, zero-test discovery).
```

Here `p` is the admitted interval or integer scalar width, not automatically 8 or 16. `L=1` needs only the initial total and `L=0` no moments; do not allocate group history unnecessarily. The zero route retains only `p_initial*G*F*d` while checking initial groups, then releases it before row output. For `G=1`, current group and total coincide: the uniform baseline stores only `L*F*d`, not `(L+1)*F*d`.

| Quantity, ordinary nonzero grouped case | Original history replay | Grouped closure |
| --- | --- | --- |
| Logical membership visits, including output | `2L*m_B` | `2L*m_B` |
| Feature-vector gather/scatter units | `m_B*d*L*(L+3)/2` | `m_B*d*(3L-1)` |
| Additional group reduction/scaling | none | at most `(2L-1)*G*F*d` scalar operations |
| Local node replay work | `O(n*d*L^2)` | `O(n*d*L)` plus normalization |
| Initialization rows regenerated | `(L+1)*n` | `2n` |
| Retained total history for output | `L*F*d` | `L*F*d` |

Thus the new route improves **arithmetic per pass, not the number of ordinary topology passes**. The feature-unit saving is `m_B*d*(L-1)*(L-2)/2`, before added grouped operations. At `L<=2` there is no such saving; large `G` can lose badly. At `L=8` the coefficients are 44 versus 23, not a claimed 1.91x elapsed-time speedup. Integer multiplication cost depends on `W`; compare both schedules at matched exact widths, not grouped integers against an uncertified floating baseline.

The zero route is different: one membership scatter, then row output; its **matched uniform-feature baseline has the same optimization**. When all active feature cardinalities equal `t`, every active `d_i=-1/(t-1)` and

```text
H_(timestep+1) = d_0 H_timestep + V^T U H_timestep.
```

This is precisely the known single-group matrix-free baseline, with the same raw layers and normalization. The fourth listing independently implements it and compares all history cells. No uniform-feature win is attributed to inventing this recurrence. Multiple diagonal groups extend that schedule, but the algebra remains established structured recurrence; useful novelty would have to lie in a demonstrated bounded completion/resource crossover.

### Preparation, Refresh And Failure Costs

Discover exact diagonal groups from streamed `(q_i,s_i)` records with a capped dictionary or a charged external sort/join; store an explicitly sized group ID in the prepared row stream. Group/feature discovery is not an uncharged `n`-map. Compute `S`, `M`, the capped common `Q`, integer widths, output precision and all workspace limits before starting the nonzero exact route. The zero certificate can be attempted without constructing `Q`; failed discovery plus a different schedule both count in the lifecycle.

Zero-route admission must actually precede general-route Q/history admission, not merely stop reading Q after it has been allocated. Reserve initial group cells using `W_initial`, initialization/metadata cursors and separately sufficient output workspace; validate the zero certificate; only on failure consider the general route. The follow-up review's 47-node unit-star fixture fits eight-bit initial cells but not the general common denominator. The revised fourth probe now has a distinct zero-only preparer profile and exercises this exact ordering. Its fixture preprocessing still uses small in-memory source arrays and is not the production external preparer.

Retained disk adds row group IDs, selected total histories and optional complete group checkpoints. Under integer width `p`, a retained checkpoint contains `p*G*F*d`, in addition to any retained `p*L*F*d` output history. Old/new checkpoint and output generations count together. The original 50 GB **portfolio-wide** retention equation still applies. Binary output still needs at least `f_out*n*d` bytes: `n=100M,d=128` binary32 is 51.2 GB before IDs and cannot be retained within 50 GB.

Any membership/count/seed/scale change requires new exact groups or validation of their unchanged assignment, new coefficients and regenerated moments. This revision does not supply cheap incremental embeddings. Singleton activation can alter neighbors' diagonals and destroy the zero certificate. Reuse neither old group IDs nor an old zero proof by node identity alone. Restart from complete versioned group checkpoints or redo initialization/history; mid-pass overwritten groups cannot be treated as complete. If neither grouped nor original replay fits, admission fails rather than silently spilling an unbounded group plane.

When `G` is too large but `Q`, `W` and `p*L*F*d` fit, the original history-replay schedule can itself use the integer coefficients `(a_i,b_if)` and scaled histories `J_t`. Its existing induction and the same finite output-completion guarantee apply, without any `G*F*d` array, but with the original quadratic-in-depth work. The grouped route is an optional faster constructor, not permission to weaken an otherwise admitted exact query. The fourth probe executes the grouped and uniform constructors; this integer original-replay fallback is specified by direct substitution, not separately exercised there.

## RAM, Work And I/O Equations

Let `m_B=nnz(B)`, `T_B` be incidence-membership payload bytes, `T_row` the complete per-node directory/coefficient/ID/init-column stream, `f_out` output scalar bytes, and `p` bytes per stored numerical scalar. The resident base schedule uses

```text
M_history = p*F*d*L                 # current accumulator included
M_local   <= p*d*(L+3) + bounded arithmetic/rounding scratch
M_feature = C_feature*F             # weights, counts, feature directory
M_total   = M_history + M_local + M_feature + B_decode + B_IO
            + B_output + B_ID + B_runtime + B_residency.
```

There is no resident `n`-length degree array: row `q_i,s_i,g_i` and IDs are streamed. Feature counts can be external during preparation, but the selected query plan reserves its feature metadata. Per-thread copies of `Z_t` are prohibited unless separately admitted; the reference schedule is single-worker and deterministic. All buffers and mapped residency count toward the physical 4 GB budget, not only managed allocations.

For the simple gather/rescan scheme, `L` history passes plus one output pass read at most

```text
Topology/row logical reads <= 2*L*T_B + (L+1)*T_row, for L>=1,
    before page alignment, ID decoding and restart rereads.
Output writes              = f_out*n*d + ID/format/checksum bytes.
History checkpoint writes <= p*F*d*L if each finished layer is persisted once.
```

The `2L` membership factor is important: late-stage history construction needs gather then scatter, and a high-degree row cannot be buffered for free. Output is already row-major; no dimension-stripe transpose is required. The feature arrays are RAM-resident on this admitted path, so their accesses are memory traffic rather than hidden disk reads. They can dominate CPU/memory bandwidth nonetheless.

Feature-vector multiply/add work is bounded by

```text
m_B*d*( L*(L+1)/2 + L ) = m_B*d*L*(L+3)/2,
```

plus `O(n*d*L^2)` local replay/normalization and `(L+1)*n*d` initialization generation in the dense-generation profile. This is a deliberate quadratic-in-depth tradeoff. Expensive hash generation is charged, not assumed negligible; sparse initialization does not imply sparse propagated feature vectors.

By comparison, D14's original full-node stripe model retains approximately `3*f*n*c + 8*n*(L+1)` bytes before degree/runtime state and executes `2*L*ceil(d/c)` logical propagation passes. A fair incidence-native D14 baseline also factors the operator rather than expanding group cliques. Compare both against that competent baseline, not only an expanded graph. Our smaller number of membership scans can lose to its lower arithmetic work when `L` is large.

### Illustrative Admitted Shape

For `n=10M`, `F=100k`, `m_B=50M`, `d=128`, `L=3`, interval `p=16`:

| Quantity | Selected bytes/work |
| --- | --- |
| Entire feature history including current accumulator | 614,400,000 B |
| Local vector slab | 12,288 B |
| Feature metadata at an explicit 24 B/feature | 2,400,000 B |
| Additional provisional I/O/decode/ID/output/runtime/residency reservation | 600,000,000 B |
| Planned worker subtotal | 1,216,812,288 B, not measured |
| u32 incidence membership payload | 200,000,000 B |
| Membership payload traffic, `2L*T_B` | 1,200,000,000 B |
| Illustrative 32 B/node row stream across `L+1` scans | 1,280,000,000 B |
| Raw binary32 embedding output | 5,120,000,000 B |
| Feature-vector weighted accumulation units | 57,600,000,000 |

The last row is why low I/O must not be mistaken for a short runtime. Interval operations add work beyond ordinary scalar multiply/add counts. Actual memory allocation and all lifecycle timing remain unmeasured. At `F=1M` with other parameters unchanged, history alone is 6.144 GB and this resident-history profile is inadmissible. At `n=100M,d=128`, binary32 output alone is 51.2 GB: it cannot be retained under a 50 GB cap regardless of replay.

## Preparation, Reuse, Refresh And Recovery

Preparation externally sorts/deduplicates raw `(node,feature)` records, computes feature counts, identifies active features and derives `q_i,s_i` in a bounded join/reduction. A feature-ordered intermediate and node-ordered final file have paid sort/read/write costs. Raw singleton provenance must remain available for refresh or be re-extracted from a charged source. No clique expansion or all-pair feature Gram build is required. Discovering a small exact factorization from an arbitrary expanded edge list is **not** supplied by this proposal.

Retained and peak disk are separate:

```text
D_prepared = unique pinned incidence/raw-provenance/ID/coefficient blocks
             + retained feature histories + retained output generations
             + checkpoint/catalog bytes <= 50e9.
D_peak = D_prepared + non-counted input + bounded sort/join runs
         + unpublished replacement blocks + staged outputs.
```

A newly staged output that becomes retained is reclassified, not counted twice at the same instant. But old output plus its distinct replacement really are two outputs. At the illustrative shape, two raw binary32 output generations already occupy 10.24 GB, before any other family, topology or checkpoints. No algorithm receives a separate 50 GB portfolio allowance.

An unchanged operator, initialization and depth can reuse history for different layer weights and for row-subset export; reconstructing and normalizing requested rows still costs work. A changed seed, supplied scale column, topology or projection invalidates history. Changing only the output precision/tolerance may reuse history only if its enclosures suffice. A larger depth extends a valid stored prefix if additional memory and scans are admitted.

A membership edit changes feature counts and can alter `s_i,d_i,U_i` for every member of an affected feature. The new histories can affect many subsequent rows. The initial refresh procedure rebuilds counts/coefficient dependencies and recomputes all history against the new immutable snapshot; no sublinear dirty-closure claim is made. An incremental identity such as `Z'_t-Z_t=(V'-V)^T E'_t+V^T(E'_t-E_t)` does not itself provide a cheap update algorithm.

Checkpoint a completed `Z_t`, not a partial unversioned accumulator. A crash resumes by reading the verified history prefix and redoing an interrupted pass. Row output checkpoints bind seed, coefficient generation, numerical mode and completed ID range. A partial row is discarded; resumed output never mixes norms or histories from different snapshots. Whole-job measurement includes extraction, validation, all retry/recovery scans, final row delivery, retained old readers and publication.

## Revised After Counterexample

| Initial attractive idea | Counterexample or challenge | Surviving correction |
| --- | --- | --- |
| Propagate only shared-feature aggregates and forget node-local randomness | Two nodes share one feature, with `R0=(1,0)`, `R1=(0,1)`. Loop-free propagation swaps them. Omitting the negative diagonal makes both `(1,1)`. | Retain the reconstructible `d_i^l R_i` term, even when memberships are identical. |
| Compute exact row norms from individual vector norms | `x=(1,0)`, `y=(-1,0)`: individual squared norms sum to 2, but `||x+y||^2=0`. | Reconstruct the actual vector. Do not assume random initial independence survives graph propagation. |
| Treat every incidence-derived normalization as equivalent | The Zhou hypergraph walk includes a feature-size divisor/self-transition convention; our shared-count graph excludes self. Overlapping features make binary and additive projections differ too. | Pin equation (1); reject incompatible projections instead of changing the mathematical problem. |
| Real-arithmetic factorization implies floating parity | In binary64, `(1+2^-54)-1=0`, while the exact nonself contribution is `2^-54`. Normalizing zero versus this positive scalar has error 1. | Interval certification, explicit zero proof, paid precision retries and honest undecided outcomes. |
| Reuse a single node-row decode for gather/scatter without accounting | A high-degree row can exceed the entire worker allowance. | Use two bounded cursor passes; charge `2L*T_B`, rather than claiming one free read per pass. |
| Low-rank moments eliminate replay cheaply | `V^T D^j U` can be dense, and global covariance can be quadratic. | Never materialize these moments; pay finite-depth local replay and explicitly retain `L*F*d` numerical values. |
| More interval precision will settle every default-profile zero | Legal K4 seed 8 gives non-singleton intervals around two raw zeros at every finite independent-coefficient precision. | Preserve exact group dependence; certify zero initial groups, or use admitted scaled-integer propagation and exact zero tests. |
| Zero total feature sum persists for any diagonal | The four-row, two-feature example above has `H_0=0` but `H_1=(1/3,-1/6)`. | Require zero for every exact diagonal group, not just their sum. |
| Few diagonal groups make exact arithmetic cheap | `U` denominators may vary within a group; their common multiple grows, and `L` multiplies required bit width. | Cap common denominator, integer width and rational-output workspace before allocating. Use original exact replay or a separately admitted plan when grouping loses. |
| Exact raw vectors guarantee any requested floating output | Nine all-one coordinates normalize to `1/3`, which no binary32/binary64 value represents within `1e-20`. | Admit a sufficient format/tolerance domain and report proven format impossibility separately from numerical ambiguity. |

The original scalar cancellation uses a legal supplied dyadic scale. The independent review's seed-8 fixture is stronger because it uses the default ternary profile; both now have executable exact completion tests, including a supplied minimum-subnormal value `2^-1074`.

## Evidence And Verification Plan

The [exact probe](similarity-embeddings-Evidence.md#embedding-probe) enumerated every binary `4 x 3` incidence matrix: 4,096 inputs, weighted features `(1,2,3)`, raw singleton retention with analytical suppression, a fixed signed three-dimensional initialization and four propagation layers. It checked 65,536 reconstructed row vectors and squared norms against separately constructed loop-free rational transition multiplication, covering 11,664 exact-zero row instances. A strengthened rerun also checked 147,456 feature-history cells built from the candidate's own reconstructed rows. All matched. These finite checks are not a proof of floating-point certification or measured resource enforcement.

**New executed schedule:** the [fourth evidence listing](similarity-embeddings-Evidence.md#grouped-exact-completion-execution) tests 180 randomized incidence graphs, exact dyadic inputs, negative/zero layer weights, `L=0..6`, both output formats, and a separate dense rational operator oracle. It checks 2,397 raw rows across these and adversarial cases, including 758 zero rows, and stages 187 complete output files. It verifies the exact membership and feature-unit equations fixture by fixture. A separate uniform-diagonal constructor matches all history cells on 135 cases. The K4 output is exactly `(1,-1,0,0)`; the zero-only route is additionally executed with the `Q` attribute removed. The varying-diagonal false-zero shortcut, impossible format tolerances, resource admission and minimum-subnormal normalization are exercised. A full rebuilt snapshot changes the seed-8 zero proof to a nonzero history; singleton activation/deactivation also produces verified refreshed output. The listing preserves the source and measurements; no production 4 GB result is inferred.

Rejection-capable next experiments:

- **A06-R1:** compare each layer, exact zero classification, full norms and final normalized rows on independent rational/high-precision fixtures; vary initial vectors, integer feature weights, isolates and overlapping features.
- **A06-R2:** implement and audit directed rounding before claiming certification; test catastrophic cancellation, underflow, overflow, signed zeros and normalization-boundary retries. One false certificate rejects the implementation.
- **A06-R3:** on a true 4 GB physical machine, sweep `F/n`, `L`, `d`, feature popularity and group overlap. Compare complete source-to-output cost against D14, a full-layer implementation and an incidence-native baseline. Report failed/undecided jobs, not just admitted successes.
- **A06-R4:** include hash generation, row directory reads, complete vector output, optional history checkpointing and downstream required format. Any later standardization or ANN index has a separate measured build/storage bill.
- **A06-R5:** refresh a feature from singleton to pair and pair to singleton; verify changed degree dependencies, old/new generation coexistence, crash/restart and a slow output consumer.

Kill the product claim if history/group state rarely fits, exact bit growth or reconstruction dominates, unresolved interval jobs miss deadlines, or a factor-aware baseline wins after preparation and refresh. Small executable research probes were performed, not a production implementation or large-machine/4 GB benchmark.

## Final Synthesis And Hard Gap

The surviving mechanism is **diagonal-grouped moment construction plus original-row replay and normalization**. It preserves the original elimination result, replaces quadratic history-building arithmetic with a paid `G*F*d` recurrence, and adds exact dependency-zero and scaled-integer completion domains. It does not claim new structured algebra or an advantage over the matched `G=1` baseline.

The hard gap is now **economic coverage**, not absence of any exact-zero route: how often `G`, the transition-denominator width, history and rational-output workspace fit together and finish faster than the strongest incidence-aware alternative. The seed-8 failure is resolved in the tested domain, but generic interval completion, scalable refresh/recovery, physical 4 GB measurement and end-to-end crossover remain open. Novelty of this application-specific combination is unresolved.

## Alternative: Fixed-Width Lattice Replay

The new [integer-lattice history route](Embeddings-Lattice-History-Replay.md) addresses the transition-denominator-width obstacle with a different numerical/work tradeoff. Store integer feature sums, reconstruct each node's integer trajectory, form its exact self-excluded weighted numerator, then round once by exact quotient/remainder. This exactly reproduces a full-state fixed-point propagation without storing its n-by-d layers. Stochastic nonexpansion gives a raw-coordinate error bound `t*2^-b/2`; original normalized output is published only after an explicit norm/error and format check.

Feature-history scalar width is at most `2+bit_length(max(1,n*M))`, and the weighted numerator needs `2+bit_length(max(1,3*s_max*M))`, where M is the largest initial lattice integer. Neither contains Q^L. On the new 106-node/eight-feature, depth-24 prime-degree fixture, the older global-Q reservation is 652 bits and these lattice reservations are 33 bits; all normalized rows pass at tolerance 1e-5. This is a constructed precision comparison, not a measured memory ratio or victory over a component-local exact solver.

The price is quadratic-depth history replay: per-node rounding is nonlinear, so the existing grouped linear recurrence cannot be used unchanged. Exact-zero/near-zero rows still need certification or another admitted route. Both algorithms compute against the same real-valued target; the lattice plan accepts bounded error rather than claiming exact rational intermediate values. The completed [independent numerical/prior-art review](Embeddings-Defect-Prior-Art.md) supports the recurrence, adds a separate divisor reservation when M=0, and shows that the ideal norm-floor recipe does not guarantee passage of the conservative publication gate. Quantized averaging is a close established numerical precedent. Same-factor fixed-point and external-node-plane competitors remain necessary before any paper-ready claim.

## Follow-On: Compact Means And Observed Defects

[Clipped feature-mean replay](Embeddings-Mean-History-Replay.md) removes feature cardinality from each retained history scalar's range, while still paying for a temporary exact-sum plane. The update reconstructs weighted sums from quantized means and clips to the proved initial range. Its reviewed universal one-step error constant is `lambda_p=1/2+1/(2^(p+1)-1)` for p extra fractional mean bits, with matching one-step examples. For p>=1 clipping is a no-op; at p=0 it can be necessary. Two additional node-grid bits suffice for the p=0 route to match or improve the sum route's worst-case raw bound, but do not guarantee identical normalized acceptance.

The complete-output gate can also record actual mean and node-division defects in O(L) dyadic scalars and combine them with global norm minima collected during staged output. Exact coarse-grid alternating trajectories then certify where a worst-case radius refuses. Missing either defect term demonstrably produces false certificates. Small tests pass, including a rational oracle for raw measured radii and every accepted output.

The illustrative byte-packed history/conversion peak is 211.2 MB versus a matched 256 MB exact-sum reservation, with 409.6 MB extra conversion I/O. Those are partial payload calculations, not physical 4 GB execution or measured speedups. Small-cardinality exact storage, mixed precision, an external node plane and a generic same-mean replayer remain strong controls. The [mean review](Embeddings-Mean-History-Review.md) identifies quantized-history/GNN precedents and an exactly equivalent scaled-sum quantizer; novelty remains unresolved.

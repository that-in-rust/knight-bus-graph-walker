# PageRank State From Factors And Defect Covers

Date: 2026-09-20. Portfolio A01. Research candidate, not an established globally novel algorithm, production implementation, full GDS compatibility claim or measured RAM/latency result.

Separate semantic extension: [Boolean two-membership PageRank](PageRank-Boolean-Two-Membership.md)
handles native clique unions where shared edges count once and each vertex
has at most two distinct memberships. Its class-block cancellation is not
the diagonal-only Q contract below. It derives an F-coordinate solve,
positive grounded formulation, arbitrary-personalization recovery and an
original-output certificate, with 5,640 exact cases lead-replayed. Generic
block elimination matches its core. Neither this manuscript's weighted
timings nor its convergence constants transfer automatically to that graph.

## Premise And Proposed Contribution

Some analytical graphs combine regular relationships with exceptional edges. Examples include weighted shared memberships with explicit overrides, or many spokes linked to a few hubs. Ordinary compressed PageRank can reduce edge traffic while still retaining one or more changing values per vertex. Our question is different: **can a storage compiler choose factors to minimize the changing solver state, even when those factors do not make the edge file smaller?**

The candidate is a matrix-free stationary PageRank solver whose mutable state has `F + c + 1` entries, where F is the number of admitted nonnegative base factors and c is a row/column star cover of the exceptional-edge support. It supports directed weighted input, exact diagonal cancellation, nonuniform personalization and dangling vertices with incoming edges. It reconstructs original-vertex scores in a stream and certifies their global L1 error from factor-sized state.

The revised combination to investigate is: **contraction-budgeted factor selection, cover-factored exceptional edges, a bounded fused row schedule and an original-answer residual certificate**. A new release option pays for extra factors to remove harmful diagonal cancellation at selected vertices. Matrix elimination, biclique compression, PageRank residual bounds and bipartite vertex cover are established foundations. Their combination is a research hypothesis, not proof of novelty. The nearest-art table below gives specific collisions and unresolved comparisons.

## Why This Addresses The User's Problem

The whole graph need not be resident, but merely externalizing edges does not remove N-sized rank vectors. At N=100 million, two f64 vectors occupy 1.6 billion bytes before topology, ID maps, buffers or the process. If a measured source admits 1 million base factors and 50,000 defect-cover factors, two reduced vectors occupy 16,800,016 bytes. This is about 95.2x less **selected iterative state**, not a 95.2x application-RAM or Neo4j improvement.

This path is for repeated stationary ranking jobs over a suitable versioned graph. It is not the shortest route to production for arbitrary Cypher, unsupported nonlinear projection weights, approximate similarities, or graphs with no small factor state. Source preparation and full result delivery can dominate the benefit.

## The Semantic Contract

Use destination-row, source-column convention: `A[i,j]` is the nonnegative weight of edge `j -> i`. Explicitly choose duplicate aggregation, labels, direction, weights, node universe and self-loop semantics before factorization. No negative edge weights.

The admitted representation is:

```text
A = C B^T - Q + E
Q = diag(q), q >= 0
C, B, E >= 0
A >= 0
```

C and B are N-by-F sparse factors, E is an exceptional-edge matrix, and Q removes declared diagonal mass. For a loop-free shared-membership projection, C=B and `q_i=(CB^T)[i,i]`. The general theorem permits other nonnegative factorizations and an admitted diagonal correction. E is additive, not an arbitrary signed change log. Deletions from factored base edges require a valid refactorization or a separately designed signed method; do not feed negative E to this algorithm.

Let `d_j=sum_i A[i,j]`, `Dinv[j,j]=1/d_j` for positive d_j and zero otherwise, and z be the indicator of zero-degree sources. Personalization p is nonnegative and sums to one. Dangling mass redistributes to that same p. For `0 <= a < 1`, the target is:

```text
P = A Dinv + p z^T
x* = (1-a)p + a P x*
```

Empty universe is an explicit empty-result case. At a=0 return p without iteration. Isolates remain in the universe. A vertex can have zero outgoing degree and positive incoming degree; that fact invalidates the shortcut used for symmetric isolated vertices in earlier D16.

This is a stationary error contract. GDS exposes finite `maxIterations`, score-change `tolerance`, personalized source nodes and optional output scalers. An L1-certified stationary answer is not automatically its finite-iteration output or score scale. Pin an adapter contract and oracle separately. [GDS PageRank documentation](https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/).

## Step 1: Compile Defects Into Solver Factors

Build a bipartite support graph of E. Its left vertices are source copies, its right vertices destination copies, and an edge joins j to i iff `E[i,j]>0`. These are two copies even when i=j.

Choose a vertex cover `S union T`: every defect touches a selected source in S or selected destination in T. Assign each defect exactly once, destination-first:

1. For every selected destination i, create factor `u=e_i`, `v=E[i,:]`.
2. For every selected source j, create factor `v=e_j`, `u_i=E[i,j]` only for destinations not in T.
3. Remove empty factors. The sum of their outer products is exactly E.

These steps work with arbitrary nonnegative edge weights; no numerical rank approximation is involved. A defect touching both selected endpoints must not appear twice.

Within the restricted class of row/column-star factorizations, a minimum bipartite vertex cover minimizes factor count: each factor chooses an incident source or destination, so covering all nonzeros is necessary; the construction proves sufficiency. This is a direct application of established cover theory, not a new lower bound for arbitrary nonnegative factorizations. Dense bicliques can admit fewer general factors than a star cover.

**Do not hide the cover builder.** A source-only or destination-only cover is a simple admissible fallback, priced by its actual size. A maximal matching gives a cover consisting of its endpoints, of size at most twice the minimum, and a matching-size lower certificate. A large graph may still require too much state or time to find a better cover. Exact minimum cover requires a priced matching phase or an external-memory implementation, neither implemented here. Bounded budget exhaustion before a valid certificate is obtained means "this builder did not establish a plan", not "no plan exists".

Concatenate base and defect factors into U,V with r=F+c columns:

```text
A = U V^T - Q
```

Before publication, validate nonnegativity, diagonal cancellation and normalized degrees. For producer-native factors the source semantics can establish the factor identity without enumerating every expanded edge. For arbitrary supplied adjacency, discovering and verifying a small factorization is an additional nontrivial build operation. Never assume it is available for free.

## Step 2: Remove Diagonal-Only Work

A factor supported on the same single vertex i on both sides has diagonal mass m. Consume `s=min(m,q_remaining[i])`, reduce its mass by s, and subtract s from the current q ledger. Delete the factor only when its remaining mass is zero. Also remove factors with an empty side. Processing the next factor must use the updated ledger, not the original q.

Do not remove an actual self-loop carried by E or any uncanceled diagonal mass. This is a semantic simplification, not a decision that all loops are unimportant. Raw membership provenance may still be needed for future changes and other queries, so retain it outside the rank-only artifact under a separate storage charge.

This step was added after the counterexample in the verification section and corrected after independent review for partial cancellation. Masses 2 and 3 with q=3 must leave a genuine self-loop of weight 2. Removing both against the original q would change the graph. Fewer state variables alone can otherwise conceal very poor convergence.

### Step 2b: Release Harmful Cancellation Under A Contraction Budget

Singleton pruning does not address non-singleton cancellation. On the fixed graph `A=[[0,1],[1,0]]`, the rank-one representation `u=(K,1), v=(1,1/K), Q=diag(K,1/K)` makes the reduced base iteration arbitrarily slow as K grows. The problem is the representation, not the original graph.

For a requested contraction bound `a<=gamma<1` with a>0, select every active vertex satisfying

```text
q_i/d_i > (gamma-a)/(a*(1-gamma)).
```

Assign each selected vertex to a row-release set R or a disjoint column-release set C. Zero U rows R and V rows C, remove q on R union C, then restore selected ORIGINAL rows as destination stars and selected ORIGINAL columns outside R as source stars. Empty factors are removed. The exact identity is

```text
A = (I-D_R) U V^T (I-D_C) - Q_outside
    + D_R A + (I-D_R) A D_C.
```

This yields at most `r+|R|+|C|` factors and preserves all original degrees and answers. The convergence proof below now gives beta<=gamma. Every above-threshold vertex must be released to meet that particular maximum-ratio certificate within this restricted family. This is not global factor optimality or a lower bound for other solvers.

There is also a stronger uniform state bound. With W=UV^T, selected vertices q_i>d_i define a strictly column-diagonally dominant principal submatrix: W_ii=q_i+A_ii exceeds the sum of its off-diagonal column entries. That submatrix is nonsingular, so there are at most rank(W)<=r selected vertices. Releasing them yields **at most 2r factors and beta<=2a/(1+a)**. At r=0 the admitted graph is edgeless, possibly with many isolates, and its answer is p. This standard nonsingularity/rank argument strengthens the initial trace-count bound; the full proof and sharpness example are in the revision note.

Release is optional and priced. Selected dense rows, external joins, scratch and replacement generations can make it inadmissible: even O(r) restored rows can contain O(Nr) nonzeros. If the original-state reservation fits, first certify its initial answer and optionally run a bounded pilot; then compare release with Jacobi, a paid tiny-core solve and the ordinary graph solver. Never allocate an inadmissible old state for that check. A more aggressive gamma approaching a loses the useful constant-factor state bound; an already large r can also erase the advantage. Row/column orientations can have different storage and certificate costs; no optimal orientation algorithm is claimed.

The full construction, iteration-bound theorem, adversarial controls and exact saved-source checks are in [PageRank-Representation-Revision.md](PageRank-Representation-Revision.md). For K=1 in the graph above, the original initialization is already exact: blindly releasing debt worsens this case. A better worst-case contraction bound is not always a better actual run.

**No-expansion row-only option:** [PageRank-Positive-Release-Kernel.md](PageRank-Positive-Release-Kernel.md) implements the released operator algebra without materializing A[R,:]. An ordinary row pass gathers original-factor contributions from outside R and retains x_i/d_i only on R. A selected factor-major pass uses prefix/suffix sums to gather each restored row while excluding its own selected contribution; actual nonnegative self-loop weight is added separately. This avoids the dynamic subtraction of huge canceled diagonal mass. The extra membership record count is at most the original factor nonzeros, and working state is O(r+|R|). The certificate remains the same original-space one. This is the preferred storage candidate for heavy release, subject to a paid selected-order builder, numerical admission and independent review.

## Step 3: Eliminate All Entity Iterates

Define:

```text
H_ii = 1/(1 + a*q_i/d_i), when d_i > 0
H_ii = 1,                 when d_i = 0

Ubar = [U, p]                         N by (r+1)
Vbar^T = [V^T Dinv; z^T]             (r+1) by N
b = (1-a)p

X(h) = H (b + a Ubar h)
T(h) = Vbar^T X(h)
```

Solve `h=T(h)` and reconstruct `x=X(h)`. The extra scalar is the dangling mass. The default does not construct either `Ubar Vbar^T` or `Vbar^T H Ubar` as a dense matrix. The latter can have `(r+1)^2` entries despite sparse input factors. A separately admitted tiny-core solve is a legitimate competitor: allocate and charge its dense matrix, factorization workspace and numerical validation instead of banning it categorically.

### Correctness Claim

In exact arithmetic, fixed points of T reconstruct precisely the stationary PageRank solution. Conversely the unique PageRank solution maps to a fixed point by `h=Vbar^T x*`.

Proof: since `A=UV^T-Q`, the PageRank equation rearranges to

```text
(I + a Q Dinv)x = b + a Ubar Vbar^T x.
```

The left diagonal is invertible with inverse H, including dangling entries where Dinv=0. Substituting h proves both directions. This is standard elimination, derived explicitly here to make orientation, loops and dangling behavior inspectable.

## Step 4: Fuse Reconstruction And Gathering

Persist a merged entity-order row stream. Each row contains its U factors, V factors, scalar degree/correction data and required source/output metadata. Split oversized rows into bounded chunks. Iterative vectors are indexed by factor, not entity.

```text
INPUT: validated row stream, a, p, tolerance eps

read rows once to gather h = Vbar^T p

repeat:
    zero next[0 .. r]
    for each entity i in row order:
        t = dot(U[i,:], h[0 .. r-1]) + p_i*h[r]
        xi = H_i * ((1-a)*p_i + a*t)
        if d_i > 0:
            for each (factor j, weight v) in V[i,:]:
                next[j] += v * xi / d_i
        else:
            next[r] += xi
    bound = a/(1-a) * sum_j c_j*abs(next[j]-h[j])
    if bound plus validated arithmetic allowance <= eps:
        stream X(h) with original node IDs; finish
    if the admitted pass/disk/deadline budget is exhausted:
        return explicit nonconvergence, not a certified answer
    swap(h,next)
```

Here `c_j=sum_i Ubar[i,j]`, and the last weight c_r=1. Store c on disk and scan it for the reduction, or charge a third resident array. If U's row exceeds a buffer, consume it in chunks to compute t before consuming V's row; do not retain the whole row. A combined interleaved format may require a second bounded reread of that row. Charge those bytes.

No N-element x array is needed: xi is a scalar consumed immediately. No dense reduced matrix is needed: the two sparse row factors apply it implicitly. Parallel copies of `next` would multiply the main state, so the first algorithm is single-owner deterministic reduction. Any parallel ownership scheme needs an additional resource and numerical analysis.

The warm start `Vbar^T p` is an extra streamed pass, not a free initialization. It avoids an unfairly slow zero start on graphs where personalization is already stationary. A saved h from a previous query is only an initial guess after changed data or personalization; validate it against the new operator.

## Original-Space Error Certificate

For arbitrary current h, define delta=T(h)-h and x=X(h). Direct subtraction yields:

```text
r_x = b + a P x - x = a Ubar delta

||r_x||_1 <= a * sum_j c_j * |delta_j|
||x-x*||_1 <= a/(1-a) * sum_j c_j * |delta_j|
```

The inverse bound `||(I-aP)^-1||_1 <= 1/(1-a)` is standard PageRank analysis. [Gleich, PageRank Beyond the Web, Section 2 and Aside 2.3](https://arxiv.org/pdf/1407.5107).

The factor identity enables the reduction without storing an entity residual. Check delta for h and return X(h), not X(next) without another justified certificate. Raw reconstruction need not sum exactly to one before convergence. Normalization is a separate output transformation, potentially requiring another pass and a revised error budget.

The inequality applies even when delta has mixed signs. It can be conservative because different factors can cancel at an entity. Small factor motion in an unweighted norm is insufficient: a factor reaching a billion destinations can amplify an apparently tiny residual enormously.

An optional additional U-row scan evaluates the tighter `a*sum_i |(Ubar delta)_i|/(1-a)` using a row scalar and a read-only difference view of next and h. Preserve BOTH vectors: evaluate delta[j]=next[j]-h[j] as needed with validated subtraction, and continue base iteration by swapping the unchanged arrays. This avoids the lossy floating subtract-and-restore round trip while still charging residual evaluation error. For the two-node all-ones graph, two minimum covers yield the same exact reconstructed answer but cheap bounds 0 and 289/60. The streamed original residual is zero for both. It is useful to choose certificate strategy independently from factor count.

For the base initialization h0=Vbar^T p, the reconstructed sequence satisfies `x_(k+1)=H[b+a(P+QDinv)x_k]`. Thus covers representing the same A and Q give identical original-space iterates. Cover choice alone does not accelerate their mathematical convergence; it changes state, work, rounding and stopping tightness. Diagonal release changes Q and can change convergence. Factor Jacobi is a different iteration and is not covered by this invariance statement.

### Numerical Admission Is Separate

The theorem is exact-arithmetic mathematics. Computing a small degree by subtracting a huge diagonal correction from a huge factor column sum can round a positive degree to zero. For example, a factor may contribute a diagonal weight beyond f64's exact-integer range while the useful outgoing weight is one. Misclassifying that vertex as dangling changes P itself, so a later residual calculation against the misclassified P cannot catch the error.

For integral inputs, use checked exact accumulation of degrees and q before conversion, or reject overflow. For general weights, use an explicitly validated accumulation/enclosure policy; factor-side sums excluding the self term, positive prefix/suffix sums and staged scalar joins are possible builder designs, with extra I/O. None is implemented or numerically certified by the current probe. The published receipt must identify the source operator, rounding policy and certificate allowance. A checksum only proves which bytes were processed, not that the represented operator is the intended one.

Subsequent restricted implementation: [native binary-incidence evidence](PageRank-Native-Incidence-Evidence.md) now constructs integer degrees from positive membership sums and certifies every published f64 score with a two-pass directed Decimal residual against the original operator. It covers the symmetric loop-free clique projection, not general weighted factors, directed exceptions or the release kernel. Its eight-mode natural experiment also shows reduced power can be much slower than competent same-factor methods and that lower iterative state need not reduce worker RSS. The older general numerical-admission warning therefore remains, with this specifically scoped exception.

If the source operator is P_source but the represented operator is P_hat, certify an induced L1 difference eta_P and a right-hand-side difference eta_b. Let R_computed plus eta_eval bound the residual of the reconstructed x against P_hat, including reconstruction/gather evaluation error. Then a sufficient source-answer error bound is

```text
(R_computed + eta_eval + eta_b + a*eta_P*||x||_1)/(1-a)
    + eta_publish.
```

eta_publish bounds subsequent output rounding. The source operator must satisfy the stated stochastic contract. A tested f64 algebraic identity does not provide these allowances. Per-factor compensated sums or intervals require per-factor storage; they cannot be hidden in constant runtime overhead. Near-singular tiny cores and Jacobi denominators require the same admission discipline.

### Convergence Claim

Let `t_i=q_i/d_i` on active sources and zero otherwise. Remove zero-column-mass factors. In the weighted norm `||h||_c=sum_j c_j|h_j|`, the linear part of T contracts with

```text
beta = max_i a*(1+t_i)/(1+a*t_i) < 1.
```

Proof: `Ubar Vbar^T = P+QDinv`, whose column sums are `1+t_i`. For `M=a Vbar^T H Ubar`,

```text
c^T M = a * 1^T Ubar Vbar^T H Ubar
      = a * (1+t)^T H Ubar
      <= beta * 1^T Ubar = beta*c^T.
```

Nonnegativity implies the induced weighted L1 contraction. This supplies existence of the reduced fixed point and a bound on residual decay. It does not imply fewer passes than ordinary PageRank: beta>=a, approaching one when canceled diagonal mass dwarfs useful outgoing weight. At q=0 the bound is a. Stronger numerical solvers are an option, but their vectors, orthogonalization and stopping proofs are not free extensions of this result.

After an admitted release with beta<=gamma, let D0=||T(h0)-h0||_c. The exact error bound after k base updates is at most `a*gamma^k*D0/(1-a)`. D0 needs an actual gather pass; use an upper enclosure rather than a rounded underestimate. For inexact updates h_next=T(h)+e, residual decay instead obeys `||delta_next||_c <= gamma*||delta||_c + (1+gamma)*||e||_c`. A numerical pass allowance must account for that error floor. The exact formula does not guarantee a hardware deadline, and full output still needs a reconstruction pass.

### Optional Ordinary Jacobi Variant

An established numerical method, not another innovation claim, gives a useful comparison. Accumulate the diagonal `m_j=M[j,j]=a*sum_i Vbar[i,j]*H_i*Ubar[i,j]` in a preparation pass. Sorted U/V row intersections compute this without constructing M. Replace the state update with

```text
h_new[j] = (T(h)[j] - m_j*h[j]) / (1-m_j).
```

Since `0<=m_j<=beta<1`, the exact denominator is positive. Keep the original T(h)-h error certificate; the update difference of this new iteration is not itself that residual. The variant costs an additional r+1 stored or streamed diagonal values. Subtraction, denominator precision and another simultaneous array have to be priced.

It also converges in exact arithmetic. With `D_jj=1-m_j` and `c'_j=c_j*(1-m_j)`, its nonnegative linear part is `D^-1*(M-diag(m))`; its weighted column ratios are at most `(beta-m_j)/(1-m_j)<=beta`. This standard splitting does not guarantee faster observed convergence from the chosen initial vector.

Indeed, the same 5,376-case probe reduced the maximum tested updates from 2,899 to 1,208, but increased total residual checks from 310,992 to 666,596. A better worst case coexisted with more aggregate work. It is an explicit RAM/pass tradeoff to evaluate, not a default speed upgrade. No wall-time or population-level tail claim follows from this artificial test set.

## A Shape Where Edge Compression Is The Wrong Objective

Consider a weighted directed star with center 0 and N-1 leaves. Every leaf links to 0, and 0 links to every leaf, with arbitrary positive weights. There are `2(N-1)` edges, and every vertex is both a source and a destination.

A source-only defect representation needs N factors; a destination-only representation also needs N. The mixed cover `{source-copy 0, destination-copy 0}` needs only two. It therefore needs two rank-factor values plus one generic dangling slot instead of N rank values. Leaf scores can still differ because their incoming weights and personalization differ.

The star factors contain about `2(N-1)+2` scalar nonzeros: essentially **no edge-storage compression**. Yet the changing solver state falls from O(N) to O(1). Row compression, a specialized star solver or generic low-rank elimination can also exploit this graph; those are required controls. The example establishes the distinction between edge and state objectives, not that existing solvers cannot discover it.

Indeed, if w_i are the normalized center-to-leaf weights, a closed-form incumbent uses `x_0=[a+(1-a)p_0]/(1+a)` and `x_i=(1-a)p_i+a*w_i*x_0`. It needs no iterative solve and streams the distinct leaf answers. A weighted star is not evidence of a new competitive advantage for our generic factor solver.

For arbitrary graphs the minimum cover can be N. Then this method offers no state reduction and may add overhead. Declare a fallback instead of manufacturing a universal win.

## Resource Model

Let L be encoded merged-row bytes per full pass; Z be total factor nonzeros; r be factor count; B be admitted row/output buffers; C be runtime, metadata and explicitly charged caches. Scalar widths below are f64 examples.

| Quantity | Bound or accounting rule |
| --- | --- |
| Core iterative payload | `16*(r+1)` bytes for h and next |
| Optional resident certificate weights | Additional `8*(r+1)` bytes, otherwise streamed |
| Cancellation release | At most one extra factor per selected vertex; price restored adjacency, joins, scratch and all rereads separately |
| Positive row-only release | No expanded restored adjacency; additional selected membership order<=Z records; conservative kernel scalar payload `2*r0+r+7*k+4`, plus IDs/maps/certification/runtime |
| Optional tighter certificate | One additional U-row pass; h and next preserved, delta evaluated through a validated read-only difference view |
| Optional validated arithmetic | Explicit per-factor compensation/enclosure bytes plus accumulation and output allowance |
| Optional tiny core | At least `8*(r+1)^2` scalar matrix bytes, plus factorization, right-hand sides and validation workspace |
| Other active memory | B+C; factor dictionaries, ID conversion and worker stacks must fit here or be streamed |
| Per-pass arithmetic | O(N+Z+r), with chosen numerical accumulation cost |
| Per-pass row traffic | L, plus oversized-row rereads and certificate-weight scan |
| Initialization | One row pass for `Vbar^T p`, plus needed source validation |
| Full result | One reconstruction pass, N emitted scores/IDs; no free output sorting |
| Prepared data | U/V rows, degree/correction columns, IDs, provenance, versions and optional result artifacts |
| Builder | External sorting/joins plus actual factor-discovery/cover cost; not included in L |
| Refresh | Recompute changed degrees/factors and validate cover identity; admit physical replacement and pinned generations |

Example arithmetic, not a dataset measurement: at L=24 GB and assumed effective row service 500 MB/s, one pass requires at least 48 seconds of row service. One hundred passes require 4,800 seconds before output and other work. Saving 95x selected state cannot turn that into a millisecond query. Build and result costs may erase the benefit if only one job is run.

Use total elapsed time `T_source + T_build + T_init + J*T_pass + T_output + T_refresh_amortized`. Record cold and warm runs separately. A retained file budget of 50 GB does not also grant 50 GB to scratch, a second generation and an output copy.

## Rubber-Duck Revision And Actual Probes

### First tempting design

Extend D16 by adding one state value per exceptional source, omit a general dangling equation, retain all supplied factors and start from zero. This sounds economical but has four defects:

1. Source support can be huge even when a two-sided cover is tiny.
2. Directed dangling vertices receive incoming mass, unlike isolated vertices in the earlier symmetric derivation.
3. Diagonal-only factors can slow convergence while contributing no real edges.
4. Comparing a zero-start factor solve against a good incumbent initialization exaggerates pass costs and can conceal trivially stationary cases.

### Concrete failures and corrections

For graph `0 -> 1`, a=.85 and uniform p, correct scores are approximately `(0.350877193, 0.649122807)`. Applying the symmetric isolated-dangling shortcut gives `(0.130434783, 0.241304348)`, whose sum is wrong. The general dangling state fixes the equation.

For one vertex with a real unit self-loop and base factors `C=[2,0,3,1]`, `B=[2,0,3,0]`, the base contributes 13 units of fully canceled diagonal mass. The original zero-start algorithm needed 10,434 updates to meet the test certificate at a=.97 even though the answer is exactly 1. Removing canceled singleton factors and initializing from `Vbar^T p` avoids this artificial work.

### Executed mathematical probe

An ephemeral JavaScript oracle compared independent expanded-matrix PageRank with the factor reconstruction. It covered all 512 directed binary three-vertex matrices including loops, three personalization vectors and damping .1/.85/.97: 4,608 cases. It additionally generated 192 seeded weighted, directed factor-plus-defect shapes with 1-8 vertices and 0-4 base factors at a=0/.1/.85/.97: 768 cases. Seed 20260920; final generator state 1609677230.

| Check | First version | Revised version |
| --- | --- | --- |
| Graph/parameter cases | 5,376 | 5,376 |
| Residual-identity checks | 1,612,996 | 310,992 |
| Error-bound checks | 1,612,996 | 310,992 |
| Largest residual-identity discrepancy | 4.974e-15 | 1.157e-15 |
| Largest recorded final solution L1 error | About 1.000001e-10 | 9.995e-11 |
| Maximum updates in a tested case | 10,434 | 2,899 |

The combined revision changed both pruning and initialization; this is not an attribution experiment separating their effects. It is not a wall-time benchmark. The remaining 2,899-update case is evidence against a universal speed claim. The oracle uses double precision and a 1e-10 comparison allowance, not interval-certified arithmetic. The test code and reproducibility details are in [PageRank-Probe-Evidence.md](PageRank-Probe-Evidence.md).

The independent review additionally exposed non-singleton slowdown, partial-Q coverage gaps and cover-dependent certificate conservatism. The saved exact-rational revision probe passed 216 original and 216 released-operator direct-solve/residual cases, 274 release orientations, 3,168 selected-set checks and 2,112 cumulative singleton ledgers. It includes 36 partial-Q cases and arbitrary signed factor states, with no tolerance added to exact identities. Float controls at K=1,000 reduced updates from 70,159 to 140 by releasing one vertex, while K=1 regressed from zero to 141 updates under unconditional release. These results justify conditional planning, not a universal speedup or a numerically certified f64 implementation.

A subsequent sparse positive-kernel probe passed 128 exact warm starts, 128 signed-state comparisons, 128 nonnegative-state comparisons and 640 positive trajectory updates against the explicitly expanded released factors, plus an exact K=2^54 case. The kernel reads sparse row/group fixtures and does not construct an N-vector or restored adjacency in its update function; dense matrices exist only in its outside oracle. No disk-layout or measured machine-memory claim follows.

## Nearest Prior Art And Novelty Exposure

| Primary source | What it already establishes | Candidate delta and remaining collision risk |
| --- | --- | --- |
| [Francisco et al., Graph Compression for Adjacency-Matrix Multiplication, 2022](https://koeppl.github.io/bin/paper/sncs22graph.pdf) | Computation on compressed graph representations; biclique plus residual-edge multiplication. Its displayed biclique procedure allocates an entity-sized result vector. | Investigate elimination of the entity iterate via state-priced factors and cover-factored residuals. This is not the first factor-aware PageRank or compressed multiplication. |
| [Karande, Chellapilla and Andersen, Speeding Up Algorithms on Compressed Web Graphs, 2009](https://www.internetmathematicsjournal.com/article/1489-speeding-up-algorithms-on-compressed-web-graphs/attachment/4433.pdf) | The independent reviewer inspected the full journal version: Proposition 3.1 and Section 4.1 include compressed multiplication and a stationary real/virtual-node method. | Its inspected stationary state includes real nodes. Our r-state elimination is a potential systems distinction, not the first stationary PageRank on compressed graphs. See the independent review for access and scope. |
| [Shen et al., Off-diagonal Low-Rank Preconditioner for Difficult PageRank Problems, 2019](https://doi.org/10.1016/j.cam.2018.07.015) | Row-structure low-rank factorization and preconditioning; reports preprocessing, solve and memory costs. | Our claimed distinction cannot simply be low-rank PageRank. Need comparison of exact reduced state, cover choice and streaming certification with their elimination/preconditioning controls. |
| [Shen and Carpentieri, Multi-Step Low-Rank Decomposition of Large PageRank Matrices, 2021](https://doi.org/10.3233/FAIA210212) | The independent reviewer inspected the eight-page paper, including capacitance dimension on page 401 and whole-matrix `A=D+FH` on page 404. | Our reduced matrix is a standard capacitance/Schur specialization. Neither reduced state nor factor count as a memory objective is new. Investigate only the priced release/compiler/certificate combination against this strong comparator. |
| [Weak dangling block reordering and multi-step block compression for efficiently computing and updating PageRank solutions, 2025](https://doi.org/10.1016/j.cam.2024.116332) | Publisher abstract identifies sparsity-based reordering, multistep compression and a WDBMC preconditioner for computation and updates. | High-priority novelty blocker for any broad elimination/update claim. Full fetch returned 403; inspect author manuscript before claiming distinction. |
| [Recursive reordering and elimination method for efficient computation of PageRank problems, 2023](https://www.aimspress.com/aimspress-data/math/2023/10/PDF/math-08-10-1282.pdf) | The lead inspected the abstract and introduction: recursive five-type reordering, two-stage elimination and analysis of spectral changes are disclosed. | Another direct comparator for broad structure-aware reduced solves. Method pages were not retrieved successfully in this pass; no claim about absence of our release rule follows from that access gap. |
| [Gleich, PageRank Beyond the Web](https://arxiv.org/pdf/1407.5107) | Stationary formulation, dangling variants, convergence and original-space residual-error bounds. | We reuse these foundations. The factor-sized residual identity is derived for the admitted compiler representation; it is not a new PageRank definition. |
| [Repository D16](../research_4gb_20260919/Feature-State-PageRank.md) and [D17](../research_4gb_20260919/Feature-Refresh-Certificates.md) | Symmetric incidence-derived state elimination and restricted refresh residuals. | New scope is directed weighted factors plus positive defect covers and general incoming-to-dangling handling, with explicit state-vs-edge selection. Refresh remains a separately priced rebuild/solve, not an inherited automatic incremental theorem. |

Bipartite cover duality, matrix elimination and a weighted-norm estimate are standard tools. No worldwide search can prove global novelty here. The contribution must survive direct algorithm comparisons, not an absence-of-search-results argument.

## Decisive Research Experiments

1. Separate state count, encoded edge bytes and end-to-end peak RAM. Include weighted stars, biclique-plus-defect graphs, shared-membership projections, skewed weighted cases and random graphs with no compact factors.
2. Compare with ordinary CSR PageRank, compressed factor-aware multiplication retaining entity vectors, established low-rank/elimination methods, and a specialized star solver. Use equal stationary error, not unequal maxIterations.
3. Ablate source-only, destination-only and mixed cover; then separate diagonal pruning from initialization. A cover improvement must pay for its builder.
4. Force ranks/state over RAM budget, very wide rows, large personalization files, slow output, process interruption, pinned old snapshots and insufficient live disk.
5. Measure whether the measured reduction in core state allows a useful completed job on the target machine. If required scans miss its deadline, report a capacity tradeoff, not a speed improvement.

## Honest Assessment

The derivation and small tests support a correct reduced-state mechanism under its precise nonnegative factor contract. The strengthened research angle is **joint state/contraction-budgeted factor compilation**, including useful factors that do not compress edges and selective release when a compact representation converges poorly. The current evidence does not establish publication novelty or real-dataset benefit. [Independent review](PageRank-Independent-Review.md) now includes the release and positive kernel, strengthens the factor bound to 2r, and identifies remaining planner/numerical/resource limits. The numerical implementation, closest newer elimination paper and actual bounded storage performance remain unresolved before a paper-ready or production claim.

The new [release staircase](PageRank-Release-Staircase.md) extends the m=1 guarantee to integer state tiers: `(m+1)r` factors, contraction at most `a*(m+1)/(m+a)` and a bounded candidate-emission stream. It includes a sharpness proof, executed exact probes and a completed independent challenge. The review's threshold-rounding, duplicate-coalescing and candidate-versus-execution-index distinctions are incorporated. This broadens the concrete compiler contribution without claiming that elementary convex-mixture reasoning itself is new.

The [anisotropic extension](PageRank-Anisotropic-Release.md) allocates different witness budgets to factors using pointwise source-mixture envelopes, with a strict full-rank example improving the uniform guarantee. It also derives an exact cut for fixed-set factor-survival pricing and a positive mixed row/column kernel. Its 64 budget fixtures, 64 cut-oracle cases and 50 kernel residual checks were replayed successfully by the lead. This is not a universal improvement: pure factor mixtures can force the uniform budget, empty-star optimization falls outside the cut theorem, and more phases may cost time.

The [reservation frontier](PageRank-Mixed-Reservation-Frontier.md) then shows that factor-count minimization can miss the smaller-buffer plan even when all restored stars are nonempty. A cut gives a proved scalar admission interval, not exact byte optimality. The [factor-conditioned planner](PageRank-Factor-Conditioned-Planner.md) closes that interval for an admitted exact search: enumerate at most 3^g disappearance witnesses, derive the exact surviving-factor minimum at every row count, and optimize the fixed kernel's scalar formula including both cheaper pure specializations. Completed independent review supports the proofs, identifies the omitted all-column option and tightens fractional-budget admission. Revised finite frontier/reconstruction tests pass. Generic conditioning reproduces the planner, and the survival objective is a standard directed hypergraph cut. The research angle is the combined compiler/operator/resource contract, not invention of conditioning, cuts or cardinality optimization.

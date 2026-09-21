# Independent Review: Cancellation-Capped Residual Envelopes

Date: 2026-09-21. A04 secondary mathematical candidate, not a native-protocol
change. Scope: proofs, adversarial examples, primary literature, and a small
independent exact checker embedded below. No native workloads, benchmarks,
probe/receipt changes, commits, or implementation imports.

Read [Residual-Degree Envelopes](Communities-Residual-Degree-Envelopes.md) and
its linked [independent review](Communities-Residual-Degree-Review.md).
There is no literal `research_algorithms_20260920/Review.md`; the linked review
is the companion interpreted here. No code discovery was necessary.

## 1. Premise Check and Verdict

**Yes, for the documented reduce-by-minimum weighted Misra-Gries (MG) update,
the cap can substantially strengthen the degree-only interval with O(K)
scoring work and no pair-support map. It is not uniformly informative.**

- With fixed counter capacity b >= 1, total cancellation D, residual pair
  weights x_ij, and residual edge mass M, `0 <= x_ij <= D` and `M=(b+1)D`.
- D is redundant metadata: recover it as `sum_i r_i / (2*(b+1))`. Exploiting
  it changes the certificate, not the information stored by this builder.
- A two-pass per-community construction below gives `[Lcap,Ucap]` contained
  in the existing sharp degree-only interval, using O(K) arithmetic operations
  and O(number of communities) scratch records. Retained-score work is O(b).
- It can give an exact interval where degree-only gives `[0,M]`, or halve
  width in a family with b=K-1 and a possible dense residual support.
- It is only an outer interval for the capped completion problem. A four-node
  example separates it from the exact capped LP; another pair of inputs has
  the same complete summary but original modularities -1/2 and +1/2.
- The MG facts are classical. Capped fractional degree realization is also
  classical. The candidate is an inexpensive application-specific certificate
  refinement; neither novelty nor native usefulness has been established.

Lenses used: sketch accounting, fractional realization/LP, resource accounting,
and adversarial information loss. Candidate approaches were simple pair-count
caps, stronger vertex-budget sums, and exact capped optimization. The middle
option is the primary bounded sidecar recommendation, not an implementation
request or authorization to modify the lead's frozen experiment.

## 2. Assumptions and Cancellation Proof

Use the input contract in the envelope note: nonnegative rational weights,
undirected canonical non-loop pair keys, arbitrary repeated contributions,
fixed b, exact arithmetic, and all coarse loops retained separately. Every
positive update either adds to a tracked counter, inserts into a vacant slot,
or, on an absent key with exactly b counters, cancels

`delta = min(incoming weight, minimum tracked counter)`

from each tracked counter and from the incoming contribution, retaining any
incoming remainder. Let `D=sum(delta)` over these events. Do not confuse D with
the largest decrement, the number of events, or the mass removed from all keys.

At cancellation event t let S_t be the b tracked keys plus the absent incoming
key. These are exactly b+1 distinct unordered pairs. Addition/insertion does
not change the discrepancy between true mass and stored counters. The event
adds delta_t to that discrepancy on each key in S_t. Induction therefore gives

\[
x_e=w_e-h_e=\sum_t\delta_t\,1[e\in S_t],\qquad
0\le x_e\le D,\qquad
M=\sum_e x_e=(b+1)D=\tfrac12\sum_i r_i.
\]

This also applies to a pair that remains tracked: retained counters are lower
estimates, not necessarily complete edges. Absence of a final counter does not
mean absence of the pair. A positive final counter does not imply zero residual
on that pair. Appending retained edges after cancellations can make the total
original pair weight exceed D; the cap is on **residual** weight only.

The factor is the configured capacity b+1, not final occupancy+1. If fewer
than b+1 distinct pairs exist, positive cancellation cannot occur. Necessarily,
for D>0, residual support has at least b+1 pairs and
`r_i <= (K-1)D`, in addition to the degree-only condition `max r_i <= M`.
These checks are necessary, not sufficient, for arbitrary capped metadata.

If D=0 then M=0 and every residual degree is zero. Zero-degree vertices may be
removed for the residual calculation; the formulas below also handle them
without removal. For b=0 use the existing degree envelope: the special
discard-everything builder does not supply this cancellation history. Do not
transfer the exact mass identity without proof to variable budgets, deletions,
lossy merges, median-decrement/pruning variants, independently sketched rows,
or floating-point/clamped arithmetic. A certified upper cap alone still
suffices for Section 4, but recovering it as M/(b+1) requires this update rule.

### How Much Information Is Missing After These Invariants?

For arbitrary rational contribution splitting and order, the capped model is
not merely a larger relaxation of possible MG outputs: it describes all
possible residuals for the specified `(r,D,b,h)` metadata, ignoring any extra
known original support or order.

To see this, suppose x is a loopless nonnegative rational edge vector with
`x_e<=D`, `sum_e x_e=(b+1)D`, D>0. Normalize y=x/D. The polytope

\[
0\le y_e\le1,\qquad \sum_e y_e=b+1
\]

is the convex hull of indicator vectors of (b+1)-element sets. Here is an
elementary justification: a nonintegral point cannot have just one fractional
coordinate since its sum is integral; two fractional coordinates can be moved
in opposite directions until a bound is met, expressing the point as a convex
combination of points with fewer fractional coordinates. Repeating gives a
finite rational convex combination. This is the standard hypersimplex fact,
not a new polytope result.

For each term of weight lambda, feed its b+1 distinct keys with equal weight
`delta=D*lambda`, starting with empty counters. The first b updates fill the
table and the last cancels every counter to zero. Concatenate these batches.
Finally append the desired h on its at most b keys, with no further
cancellation, and append any exact loop mass separately. This constructs
exactly x+h, D, and final h. Its residual degrees are the prescribed r.

Consequently a full capped LP would give the sharp summary-only score interval
under this flexible-stream information model. This construction is an
existence proof, not a promise about a particular frozen stream's order,
number of records, unit-update restriction, or source graph support.

## 3. Baseline and Simple Pair Counts

For a candidate partition P into nonempty communities C, write

\[
M=\tfrac12\sum_i r_i,\quad s_C=\sum_{i\in C}r_i,\quad
a_C=\max_{i\in C}r_i,\quad \ell_C=(2a_C-s_C)_+.
\]

Let I be total residual internal mass, X=M-I total crossing mass, and e_C the
boundary mass of C. Each crossing edge contributes once to each of its two
communities, so `sum_C e_C=2X` and `e_C<=X`.

The existing sharp degree-only bounds are

\[
L_0=\max(0,\max_C s_C-M),\quad
U_0=M-\max(\max_C\ell_C,\tfrac12\sum_C\ell_C).
\]

With n_C vertices in C, the immediate cap bounds are

\[
I_C\le D\binom{n_C}{2},\qquad
e_C\le D n_C(K-n_C),\qquad
e_C\ge\max(\ell_C,s_C-D n_C(n_C-1)).
\]

The upper bound on I_C counts each unordered pair once. Using `Dn_C(n_C-1)`
as internal **mass**, rather than twice internal mass, would lose a factor of
two. Zero-degree vertices can be excluded from these counts. Pair-count
constraints themselves need no enumeration of pairs, but the following
degree-aware version is at least as strong and still linear work.

## 4. A Linear-Work Degree-Aware Cap Envelope

Form the following scalars in two passes over the residual-degree array and
candidate labels. `[z]_+=max(z,0)`, and all empty maxima/sums are zero.

\[
t_i=\min(r_i,D),\qquad T_C=\sum_{i\in C}t_i,\qquad T=\sum_i t_i,
\]
\[
\alpha_C=\sum_{i\in C}[r_i-(T_C-t_i)]_+,\qquad
q_C=\min\left(2M-s_C,\ \sum_{i\in C}\min(r_i,T-T_C)\right).
\]

Define A=sum_C alpha_C, B=sum_C q_C. Then

\[
X_{\min}=\max(\max_C\alpha_C,A/2),\qquad
X_{\max}=\min(M,B/2,B-\max_C q_C),
\]
\[
\boxed{L_{\rm cap}=M-X_{\max},\qquad U_{\rm cap}=M-X_{\min}.}
\]

These formulas assume that an actual capped residual exists. With arbitrary
unverified input, an inverted interval signals inconsistency but a nonempty
interval does not certify feasibility. Do not silently clamp invalid metadata.
For example, `r=(3,3,1,1)`, D=1, M=4 satisfies both elementary feasibility
checks in Section 2, and the one-community linear formula returns `[4,4]`.
But the two degree-three vertices require degree six between them and the
others: their own pair supplies at most two degree units and the other two
vertices supply at most two more. No capped completion exists.

### Proof of Validity

For every pair ij, `x_ij<=min(D,r_j)=t_j`. Hence the internal degree at i in C
is at most `T_C-t_i`. Its external degree is at least
`[r_i-(T_C-t_i)]_+`. Summing gives `e_C>=alpha_C`.

Likewise the external degree at i is at most both r_i and `T-T_C`. Summing
gives the second term of q_C. Boundary mass cannot exceed the entire degree
outside C, namely `2M-s_C`, giving `e_C<=q_C`.

Since `sum e_C=2X` and each `e_C<=X`, the lower boundary bounds imply
`X>=A/2` and `X>=max alpha_C`. Upper boundary bounds imply `X<=B/2`.
For any fixed community H every crossing edge has an endpoint outside H;
therefore `X<=sum_{C!=H}e_C<=B-q_H`. Taking H with largest q proves
`X<=B-max q`. Together with X<=M this proves the displayed envelope.

The complementary endpoint constraint is necessary: independently summing
community internal maxima/minima can overlook matching of boundary mass at
both ends. Omitting it gives a weaker bound, not a justification for claiming
that the independent community extrema are jointly attainable.

### Proof That It Never Weakens Degree-Only

For a vertex attaining a_C, `T_C-t_i<=s_C-a_C`, so
`alpha_C>=ell_C`. Thus `Ucap<=U0`. Also
`q_C<=min(s_C,2M-s_C)`. If s_H>M for one community H, then

`B <= (2M-s_H) + sum_{C!=H}s_C = 2*(2M-s_H)`.

Thus `Lcap>=s_H-M`; in all cases `Lcap>=0`, proving `Lcap>=L0`.
Feasibility supplies `Lcap<=Ucap`. The alpha sum also dominates the simpler
`s_C-D*n_C*(n_C-1)` lower boundary bound, while q_C incorporates its
`D*n_C*(K-n_C)` upper bound.

These are sharp constraints only for the **relaxed community boundary model**
`alpha_C<=e_C<=q_C` with unrestricted intercommunity matching, not for all
capped vertex pairs. For that scalar model the two X endpoints are attained:
start at alpha for the minimum and raise other blocks to balance a dominant
block; start at q for the maximum and lower a dominant block to the sum of
the others. A known feasible vector ensures the necessary lower/upper room.
The extra X<=M does not obstruct this because B<=sum s_C=2M.

### Original Modularity

Keep the existing exact retained internal mass J(P), original W>0, original
degrees d_i, and chosen resolution gamma. Only substitute the residual bounds:

\[
Q_{\rm lo,hi}(P)=\frac{J(P)+(L_{\rm cap}\text{ or }U_{\rm cap})}{W}
-\gamma\sum_C\left(\frac{\sum_{i\in C}d_i}{2W}\right)^2.
\]

This holds simultaneously for every partition, including adaptively selected
ones, by a deterministic inequality for each completion. It is not an
optimizer guarantee. Improved upper bounds may improve a candidate-family
regret certificate without admitting any new lower-bound-certified improvement.
W=0 still needs a separate product convention; unknown residual loops or
negative weights invalidate this model.

## 5. Witnesses, Gaps, and Non-Improvement

All vertices below have positive degrees. All explicit edge weights are one
unless stated otherwise. All examples have h=0, no loops, and gamma=1.

### A. Complete Collapse of a Degree-Only Interval

Take K4, `r=(3,3,3,3)`, b=5, D=1, M=W=6, and partition `{0,1}|{2,3}`.
Six distinct edge updates cause one cancellation and leave an empty summary.
Degree-only gives `[L0,U0]=[0,6]`. Each community has alpha=4 and q=4, so
`[Lcap,Ucap]=[2,2]`. In fact the cap and degree three force every pair to
have weight one. The score interval contracts from `[-1/2,1/2]` to `[-1/6,-1/6]`.
This tiny example demonstrates correctness and size of improvement, not
asymptotic compression: its configured b is nearly the pair count.

### B. Constant Improvement with Linear Counter Budget

Let K>=4 be even, b=K-1, D>0, all r_i=2D, M=KD, and partition into K/2
pairs. Degree-only gives `[0,KD]`. Each pair has alpha=2D, q=4D, and the
new envelope is `[0,KD/2]`. Both endpoints are feasible under the cap:

- Upper: put weight D on each internal pair and give every vertex one
  external edge of weight D via a perfect matching avoiding those pairs.
- Lower: arrange a Hamiltonian cycle with no paired vertices consecutive,
  putting weight D on each cycle edge. For K=4 this is the bipartite 4-cycle.

Thus the cap removes half the old interval width; normalized score width
falls from 1 to 1/2 when W=M. The uniform complete weighted graph, assigning
`2D/(K-1)` to every pair, also has these degrees, cap, and total mass. The
batch-decomposition proof realizes it with this same empty MG summary.
Hence possible original pair support is Theta(K^2) while the configured
counter budget is Theta(K), without claiming a typical-order or runtime gain.

For the stated partition the score interval is `[-2/K,1/2-2/K]`. Its lower
endpoint is worse than the singleton baseline `-1/K`. Halving width here
therefore does **not** certify an improved candidate. This distinction is
directly relevant to the lead's separate native usefulness gate.

### C. The Linear Bound Is Not the Exact Capped Envelope

Take `r=(1,2,2,3)`, b=3, D=1, M=4, and communities `{0,1}|{2}|{3}`.
The only capped completion has edges `03,13,23,12`. Vertex 3 must use all
three incident pairs at capacity; vertex 0 is then exhausted; the remaining
unit demands at vertices 1 and 2 force edge 12. Internal mass is exactly zero.
The four distinct unit keys form one legal cancellation batch.

Nevertheless both the degree-only and linear cap formulas give `[0,1]`:
alpha is `(1,2,3)` and q is `(3,2,3)`. The missing constraint is the shared
availability of particular vertices, not a numerical rounding error. A scalar
boundary-interval calculation cannot in general enforce all pair capacities.

### D. Even the Full Capped Summary Cannot Always Help

Take eight vertices of degree two, b=7, D=1, M=W=8, partitioned into two
groups of four. Graph A is two disjoint 4-cycles inside the groups. Graph B
is a single 8-cycle alternating between groups. Each has eight distinct unit
edges, so one cancellation batch yields the identical h=0, r, b, D summary.
Their internal masses are 8 and 0, and their modularities are +1/2 and -1/2.
Both degree-only and capped intervals are the full `[-1/2,+1/2]`.

This is an indistinguishability obstruction for **any** summary-only method,
even an exact capped LP, under the stated information model. Also, if every
r_i<=D, the cap is redundant since each edge already satisfies
`x_ij<=min(r_i,r_j)`. These are distinct reasons for non-improvement.

For balanced degrees r on K vertices, `r/D=2(b+1)/K`. Small-community internal
capacity binds when `n_C-1 < 2(b+1)/K`. When communities and their complements
have ample capacity, the added inequalities may do nothing. This is a
diagnostic scaling observation, not a distributional or native prediction.

## 6. Optional Stronger Constraints and Cost Boundaries

A sorted-degree extension can tighten a boundary upper bound using both ends:

\[
q_C\leftarrow\min\left(q_C,\sum_{j\notin C}\min(r_j,T_C)\right).
\]

For a subset S of C, with h=|S|, a valid lower boundary bound is

\[
e_C\ge\sum_{i\in S}r_i-Dh(h-1)
-\sum_{j\in C\setminus S}\min(r_j,hD).
\]

Proof: degree in S can go to internal S pairs (at most Dh(h-1) degree),
to C minus S (at most the final sum), or outside C. The outside part is
bounded by e_C. For fixed h, the right side is maximized by taking the h
largest r_i, since the selection reward is `r_i+min(r_i,hD)`, increasing
in r_i. Take the maximum with alpha_C, never replace alpha with a smaller
subset bound. This is an elementary degree-capacity inequality of the same
form as classical graphicality inequalities, not a novelty claim.

Sorted prefix sums and binary searches evaluate all such prefixes, plus the
two-ended q bounds, in O(K log K) arithmetic/comparison work and O(K) scratch
records per candidate, with no O(K^2) stored pairs. The checker uses small
direct sums for this optional tier; it does not implement that sorted runtime.
Neither extension is claimed sufficient for capped feasibility or tightness.

The exact reference problem is the LP

\[
\min/\max\ \sum_{i<j:\,P(i)=P(j)}x_{ij},\qquad
\sum_{j\ne i}x_{ij}=r_i,\quad 0\le x_{ij}\le D.
\]

This has Theta(K^2) potential variables. It must not be advertised as a
bounded-memory linear-work scoring primitive merely because its input has
O(K) numbers. Implicit edges, separation, specialized optimization, or
external-memory methods would need their own proved work/storage accounting.
The exponential tiny checker below is a correctness oracle, not that solver.

### Logical Ledger for the Primary Linear Formula

| Component | Resident logical state | Work |
| --- | --- | --- |
| Existing builder and summary | O(K+b), including exact degrees/loops and at most b pair counters | Existing simple weighted builder worst case O(T_stream*b+K+b); unchanged |
| Recover D | One rational scalar, or one already recorded | O(K) sum, or O(1) from known residual mass |
| Score one candidate | O(number of communities) extra accumulators; existing K labels and summary retained | O(K+b), two degree/label scans and one retained-counter scan |
| Optional sorted constraints | O(K) additional degree/index/prefix records | O(K log K+b) per candidate as described |
| Exact capped oracle here | Exponential finite enumeration, with explicit tiny pair set | Not a production algorithm or resource baseline |

For the primary score accumulate s_C, T_C, and original d-volume in pass one;
with T known, accumulate alpha_C and q's inner sum in pass two. Reduce the
community scalars and scan retained pairs for J. No community-pair table,
original support lookup, or new cancellation history is required. Access to
stored arrays for two passes is charged, not called a new native input scan.
Rational bit growth, allocation overhead, label storage, mapping/lifting,
candidate generation, and original input residency remain separate workflow
charges. Logical records and arithmetic operations are not bytes, RSS, or time.

## 7. Nearest Primary Literature

Bounded web search performed 2026-09-21. Queries included weighted MG total
decrements, capped fractional b-matching, fractional degree realizations, and
degree-sequence partition crossing extrema. Search-result publication dates
were not used to date older papers. The following distinctions are material.

| Primary source | Checked connection; attribution boundary |
| --- | --- |
| [Anderson et al., IMC 2017, A High-Performance Algorithm for Identifying Frequent Items in Data Streams](https://conferences.sigcomm.org/imc/2017/papers/imc17-final255.pdf), Lemma 1.1 and Sec. 1.3.4 | Lemma proof explicitly counts discarded mass and bounds an item's error by total decrements. Sec. 1.3.4 gives the reduce-by-minimum weighted rule used here and attributes it to Berinde et al. These cancellation facts and the algorithm are established, not a contribution of this sidecar. Its faster variants must not automatically inherit our exact mass identity. |
| [Berinde, Cormode, Indyk, Strauss, PODS 2009, Space-optimal Heavy Hitters with Strong Error Bounds](https://people.csail.mit.edu/radu/counters.pdf) | Primary weighted-counter/tail-error lineage identified by the IMC paper. This is not evidence for a new MG error theorem or for our modularity formulas. |
| [Michael D. Barrus, 2014, On Fractional Realizations of Graph Degree Sequences](https://www.combinatorics.org/ojs/index.php/eljc/article/download/v21i2p18/pdf/), Eqs. (1.1)-(1.2), Sec. 2 | Directly writes prescribed incident sums and 0<=x_ij<=1 on every unordered pair. Scaling by D gives our capped LP constraint form. The paper focuses on graphic integer degree lists and describes fractional extreme points; our rational data and partition objective are not automatically its named theorems. This is a closer model match than uncapped degree realization alone. |
| [Fulkerson, Hoffman, McAndrew, 1965, Some Properties of Graphs with Multiple Edges](https://doi.org/10.4153/CJM-1965-016-2) | Publisher extract explicitly studies prescribed degrees and pair-specific multiplicity upper bounds on loopless graphs. Integer model; only the extract was checked, not a theorem asserted for these rational bounds. Historical evidence that degree-plus-pair-cap realization is not new. |
| [Behrend, Fractional Perfect b-Matching Polytopes. I](https://arxiv.org/pdf/1301.7356) | General prescribed-degree fractional realization framework, already used in the degree-only review. Our sketch budget b is unrelated to the vertex-demand vector conventionally called b in b-matching. The added upper capacities require the capped model, not only uncapped feasibility. |
| [Erdos, Hartke, van Iersel, Miklos, Graph Realizations Constrained by Skeleton Graphs](https://arxiv.org/pdf/1508.00542), Sec. 3.1 | Fixed degrees and partition crossing extrema are established neighboring questions; the section finds two-class simple-graph extrema using weighted perfect matching. Integral parity and realization constraints differ from this fractional interval. |
| [Sahu, 2024, Memory-Efficient Community Detection on Large Graphs Using Weighted Sketches](https://arxiv.org/html/2411.02268v1), Sec. 4.1 | Weighted MG is already used for supernode aggregation without a second exact recount there. The application combination is not new merely because keys represent coarse edges. Its per-supernode sketches differ from a single global canonical-pair budget. |

The invariant proof, linear envelope, examples, and reachability argument in
this note are explicit deductions; the sources above are not claimed to state
these exact formulas together. Failure to locate the precise linear formula
in this search is not proof of priority. A credible next claim would concern
measured certificate usefulness at a declared complete resource cost, not
renaming MG or fractional degree-completion facts.

## 8. Independent Exact Checker

This checker imports only the Python standard library and no repository code.
It checks MG identities at every prefix of short weighted streams. Separately,
it enumerates vertices of the capped LP using exact rational linear algebra
and compares the proposed intervals against the resulting exact extrema.
Enumeration does not use our envelope formulas to generate feasible graphs.

For n>=3 the incidence matrix of the complete loopless graph has rank n:
`y_i+y_j=0` for all pairs forces y=0 by a triangle. At a capped-polytope
vertex the columns corresponding to non-bound variables are independent;
otherwise a small feasible perturbation contradicts extremality. Extend them
to an n-column basis, fix every other variable to 0 or D, and solve. Thus the
enumeration covers every vertex, including degenerate ones. Compactness gives
attained linear extrema. For n<=2 solve directly. Unlike merely filtering the
uncapped vertices, this procedure includes new vertices created by upper caps.

Replay from the repository root without creating another code file:

```sh
awk '/^```python$/{copy=1;next} /^```$/{if(copy){exit}} copy' research_algorithms_20260920/Communities-Cancellation-Cap-Review.md | python3 -B
```

```python
from fractions import Fraction as F
from itertools import combinations, product


def invert_square_matrix_exact(matrix):
    n = len(matrix)
    rows = [[F(x) for x in row] + [F(i == j) for j in range(n)]
            for i, row in enumerate(matrix)]
    for col in range(n):
        pivot = next((i for i in range(col, n) if rows[i][col]), None)
        if pivot is None:
            return None
        rows[col], rows[pivot] = rows[pivot], rows[col]
        scale = rows[col][col]
        rows[col] = [x / scale for x in rows[col]]
        for i in range(n):
            if i != col:
                scale = rows[i][col]
                rows[i] = [x - scale*y for x, y in zip(rows[i], rows[col])]
    return [row[n:] for row in rows]


def prepare_capped_bases_exact(n):
    edges = tuple(combinations(range(n), 2))
    bases = []
    for chosen in combinations(range(len(edges)), n):
        inverse = invert_square_matrix_exact(
            [[int(i in edges[e]) for e in chosen] for i in range(n)])
        if inverse is not None:
            fixed = tuple(e for e in range(len(edges)) if e not in chosen)
            bases.append((chosen, fixed, inverse))
    return edges, bases


def enumerate_capped_vertices_exact(r, cap, prepared):
    edges, bases = prepared
    n = len(r)
    if n == 1:
        return {()} if r[0] == 0 else set()
    if n == 2:
        return {(r[0],)} if r[0] == r[1] and 0 <= r[0] <= cap else set()
    vertices = set()
    for chosen, fixed, inverse in bases:
        for bits in product((0, 1), repeat=len(fixed)):
            values = [F(0)] * len(edges)
            rhs = list(r)
            for e, bit in zip(fixed, bits):
                values[e] = cap * bit
                for i in edges[e]:
                    rhs[i] -= values[e]
            solved = [sum(a*b for a, b in zip(row, rhs)) for row in inverse]
            if any(x < 0 or x > cap for x in solved):
                continue
            for e, x in zip(chosen, solved):
                values[e] = x
            assert all(sum(values[e] for e, pair in enumerate(edges) if i in pair)
                       == r[i] for i in range(n))
            vertices.add(tuple(values))
    return vertices


def generate_set_partitions_exact(n, prefix=()):
    if len(prefix) == n:
        yield prefix
    else:
        for label in range(max(prefix, default=-1) + 2):
            yield from generate_set_partitions_exact(n, prefix + (label,))


def calculate_cap_envelopes_exact(r, labels, cap, optional=False):
    groups = [[i for i in range(len(r)) if labels[i] == label]
              for label in sorted(set(labels))]
    mass = sum(r) / 2
    clipped = [min(x, cap) for x in r]
    total = sum(clipped)
    volumes, excesses, lower, upper = [], [], [], []
    for group in groups:
        volume = sum(r[i] for i in group)
        clipped_sum = sum(clipped[i] for i in group)
        alpha = sum(max(F(0), r[i] - clipped_sum + clipped[i]) for i in group)
        bound = min(2*mass - volume,
                    sum(min(r[i], total - clipped_sum) for i in group))
        if optional:
            bound = min(bound, sum(min(r[i], clipped_sum)
                                   for i in range(len(r)) if i not in group))
            ranked = sorted((r[i] for i in group), reverse=True)
            for h in range(1, len(ranked) + 1):
                alpha = max(alpha, sum(ranked[:h]) - cap*h*(h-1)
                            - sum(min(x, h*cap) for x in ranked[h:]))
        volumes.append(volume)
        excesses.append(max(F(0), 2*max(r[i] for i in group) - volume))
        lower.append(alpha)
        upper.append(bound)
    degree = (max(F(0), max(volumes) - mass),
              mass - max(max(excesses), sum(excesses)/2))
    capped = (mass - min(mass, sum(upper)/2, sum(upper) - max(upper)),
              mass - max(max(lower), sum(lower)/2))
    return degree, capped


def check_weighted_stream_exact(stream, budget):
    counters, truth, cancellation = {}, {}, F(0)
    for key, weight in stream:
        truth[key] = truth.get(key, F(0)) + weight
        if key in counters:
            counters[key] += weight
        elif len(counters) < budget:
            counters[key] = weight
        else:
            delta = min(weight, min(counters.values()))
            cancellation += delta
            counters = {k: v-delta for k, v in counters.items() if v > delta}
            if weight > delta:
                counters[key] = weight-delta
        residual = [v-counters.get(k, F(0)) for k, v in truth.items()]
        assert len(counters) <= budget
        assert all(0 <= v <= cancellation for v in residual)
        assert sum(residual) == (budget+1)*cancellation
    return counters, cancellation


def compute_internal_range_exact(vertices, edges, labels):
    scores = [sum((x for x, (i, j) in zip(v, edges) if labels[i] == labels[j]), F(0))
              for v in vertices]
    return min(scores), max(scores)


cache = {n: prepare_capped_bases_exact(n) for n in range(1, 6)}
vectors = feasible = cases = improved = loose = 0
for n in range(1, 5):
    for raw in product(range(n), repeat=n):
        r = tuple(map(F, raw))
        vectors += 1
        vertices = enumerate_capped_vertices_exact(r, F(1), cache[n])
        if not vertices:
            continue
        feasible += 1
        for labels in generate_set_partitions_exact(n):
            exact = compute_internal_range_exact(vertices, cache[n][0], labels)
            degree, cap = calculate_cap_envelopes_exact(r, labels, F(1))
            _, stronger = calculate_cap_envelopes_exact(r, labels, F(1), True)
            assert degree[0] <= cap[0] <= stronger[0] <= exact[0]
            assert exact[1] <= stronger[1] <= cap[1] <= degree[1]
            cases += 1
            improved += cap != degree
            loose += cap != exact

extra_cases = 0
for r, cap in [((F(2),)*5, F(1)), ((F(2, 3),)*3, F(1, 2)),
               ((F(1, 3), F(2, 3), F(2, 3), F(1)), F(1, 3))]:
    vertices = enumerate_capped_vertices_exact(r, cap, cache[len(r)])
    assert vertices
    for labels in generate_set_partitions_exact(len(r)):
        exact = compute_internal_range_exact(vertices, cache[len(r)][0], labels)
        degree, plain = calculate_cap_envelopes_exact(r, labels, cap)
        _, stronger = calculate_cap_envelopes_exact(r, labels, cap, True)
        assert degree[0] <= plain[0] <= stronger[0] <= exact[0]
        assert exact[1] <= stronger[1] <= plain[1] <= degree[1]
        extra_cases += 1

for raw, labels, expected, truth in [
        ((3, 3, 3, 3), (0, 0, 1, 1), (2, 2), (2, 2)),
        ((2, 2, 2, 2), (0, 0, 1, 1), (0, 2), (0, 2)),
        ((1, 2, 2, 3), (0, 0, 1, 2), (0, 1), (0, 0))]:
    r = tuple(map(F, raw))
    vertices = enumerate_capped_vertices_exact(r, F(1), cache[4])
    assert calculate_cap_envelopes_exact(r, labels, F(1))[1] == expected
    assert compute_internal_range_exact(vertices, cache[4][0], labels) == truth

stream_count = 0
alphabet = tuple(product(range(4), (F(1, 2), F(1))))
for budget in (1, 2, 3):
    for stream in product(alphabet, repeat=4):
        check_weighted_stream_exact(stream, budget)
        stream_count += 1

labels = (0, 0, 0, 0, 1, 1, 1, 1)
graphs = [((0, 1), (1, 2), (2, 3), (0, 3),
           (4, 5), (5, 6), (6, 7), (4, 7)),
          ((0, 4), (1, 4), (1, 5), (2, 5),
           (2, 6), (3, 6), (3, 7), (0, 7))]
for graph, internal in zip(graphs, (8, 0)):
    assert all(sum(i in pair for pair in graph) == 2 for i in range(8))
    assert sum(labels[i] == labels[j] for i, j in graph) == internal
    assert check_weighted_stream_exact([(edge, F(1)) for edge in graph], 7) == ({}, 1)
assert calculate_cap_envelopes_exact((F(2),)*8, labels, F(1))[1] == (0, 8)
assert calculate_cap_envelopes_exact((F(0),)*3, (0, 1, 1), F(0))[1] == (0, 0)

print(f'grid_vectors={vectors}, feasible={feasible}, partition_cases={cases}')
print(f'linear_improvements={improved}, linear_not_exact={loose}, extra_cases={extra_cases}')
print(f'weighted_streams={stream_count}, checked_prefixes={4*stream_count}')
print('PASS: exact containment, refinement, witnesses, gaps, and MG identities')
```

### Executed Results

Replayed the command above on 2026-09-21 with Python 3.9.6, standard library
only. Exit status 0; exact stdout:

```text
grid_vectors=288, feasible=82, partition_cases=1100
linear_improvements=216, linear_not_exact=156, extra_cases=72
weighted_streams=12288, checked_prefixes=49152
PASS: exact containment, refinement, witnesses, gaps, and MG identities
```

Coverage: all degree vectors in `{0,...,n-1}^n` for 1<=n<=4 at unit cap,
all partitions for every feasible vector, and all partitions of three extra
vectors including a five-node case and rational cap/degree cases. The main
and optional formulas are checked for refinement and containment against
independently enumerated LP extrema. The examples in Section 5A-C are
asserted separately; Section 5D's two graphs and identical MG outputs are
also checked, as is the all-zero residual case.

The 216 strict improvements and 156 non-exact intervals are counts within
this finite diagnostic grid, not an empirical estimate of useful workloads.
Finite checks supplement the proofs; they do not measure native quality,
runtime, RSS, or typical improvement.

## 9. Final Synthesis and Open Questions

Preserve this as a secondary certificate candidate. The mathematical answer is
positive for bounded extra scoring work: the already implied cap can improve
both endpoints, with no new pair-support storage. The answer is negative for
uniform informativeness and for equality with the exact capped envelope.

The unresolved gate is whether the lead's frozen candidate family and budget
schedule encounter binding cap constraints that improve decisions or useful
certificates at acceptable complete workflow cost. Neither constructed
witnesses nor smaller intervals alone answer that. Any subsequent native
comparison must be separately authorized and must retain all no-improvement
and baseline-only cases; this sidecar does not change the frozen protocol.

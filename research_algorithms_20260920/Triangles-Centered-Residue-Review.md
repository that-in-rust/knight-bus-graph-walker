# Independent Centered-Residue Mathematical And Closest-Art Review

Date: 2026-09-21. Scope: A07 scalar counting kernel proposed in the review request, not the lead's implementation.

Read only the three requested project documents: [Shared-Residue-Review](Triangles-Shared-Residue-Review.md), [Elementary-Counting-Control](Triangles-Elementary-Counting-Control.md), and [Novelty-Baseline-Evidence-Policy](Novelty-Baseline-Evidence-Policy.md). No centered implementation, centered tests, or upcoming lead manuscript was read or imported. The sole authored file is this review. The in-memory standard-library probe is reproduced below. No long suite, public timing, production change, or commit was performed.

## Findings First

1. **Accept the exact scalar identity and streamed correction.** Under the three-mask contract, every pair of reduced periods has lcm H. Consequently a compatible pair of deviation-support residues determines exactly one t modulo H, with no missing multiplicity. Signed deviations, including magnitudes greater than one, are necessary.
2. **Accept a qualified arithmetic bound:** O(1 + R log(R+1) + R_i' R_j' + K_ij log(R_k'+2)), with O(R+1) retained integer records, provided preprocessing and enumeration obey the constructions below. Exact K is available without enumerating its pairs. Bit costs, preprocessing admission, and physical RAM are separate obligations.
3. **The least defensible performance wording is "exact work" without a named counter.** K is an exact candidate-pair count, not an exact instruction count, count of contributing triples, or runtime prediction. Mode selection minimizes each individual support cardinality, not K or total cost globally. Selecting minimum K does not necessarily minimize the displayed bound.
4. **The requested dense adversary is a valid improvement over literal repeated events, but weak novelty evidence.** It gives K=1 and Q-a-b-c+2 roots, while repeated events total 2(a+b+c). Elementary complement inclusion-exclusion already evaluates that family without period traversal.
5. **Centering, signed product expansion, sparse joins, and shared-factor CRT are all closely preceded.** In particular, AWARE explicitly uses a most-frequent default and zero-default residual computation. The correction is an ordinary weighted triangle join after an injective residue-key transformation. These facts narrow, but do not by themselves historically anticipate, the entire compact periodic kernel.
6. **Strongest legitimate boundary:** a representation-sensitive composition and admission result for native interval masks, retaining run-sized state and paying only a chosen compatible support-pair enumeration. Publication-level novelty of that precise combination is unresolved. Neither a new CRT primitive nor a universally superior join/counting method is established.

Review lenses: exact arithmetic, compressed enumeration/accounting, published provenance, and adversarial comparison. All mathematical derivations below are this review's derivations unless explicitly attributed; small checks are evidence against transcription mistakes, not substitutes for proofs.

## Premise And Contract

Exactly three positive integer periods q_i divide positive integer L. Each U_i is a Boolean subset of [0,q_i), supplied as R_i disjoint canonical half-open runs with integer endpoints. Root shifts have already been applied. Let R=sum R_i. Empty and full masks are legal. Repeated, unit, and nonminimal declared periods are legal; none creates a claim of efficient period minimization.

Validate the entire input before shortcutting. Overlaps must be rejected or normalized as Boolean union before histogram addition; adding overlapping runs as multiplicities changes the problem. Circular runs must be split consistently at zero. The scalar obligation is

```text
C(L) = #{x in [0,L) : x mod q_i belongs to U_i for all three i}.
g_ij = gcd(q_i,q_j)
h_i = lcm(g_ij,g_ik)
H = lcm(g_12,g_13,g_23)
Q = lcm(q_1,q_2,q_3)
A_i(s) = #{u in U_i : u = s mod h_i}, 0 <= s < h_i.
```

This is not a root-membership function of period H, an enumeration of satisfying roots, or a certificate of graph-output size. The graph alignment, motif summation, and local-output obligations remain outside this sidecar.

## Exact Mathematics

### 1. Reduced Moduli And Multiplicity

Each h_i divides q_i. Also gcd(h_i,h_j)=g_ij: the right-hand side divides both reduced moduli, and any common divisor of the reduced moduli divides both native moduli.

For three masks specifically,

```text
lcm(h_i,h_j)
 = lcm(g_ij,g_ik,g_jk)
 = H                                  for every i != j.
```

No prime factorization is required. The equality is not a generic many-mask property. With four native periods (2,3,5,5), the analogous h values are (1,1,5,5); the first pair has lcm 1 while H=5.

A native tuple (u_1,u_2,u_3) is CRT-compatible exactly when u_i=u_j modulo g_ij for every pair. Each compatible tuple determines one root modulo Q. Its reduced tuple determines one t modulo H, and conversely all native choices counted by product A_i(t mod h_i) are compatible. These disjoint fibers give

```text
S = sum_(0 <= t < H) A_1(t mod h_1) A_2(t mod h_2) A_3(t mod h_3)
C(L) = (L/Q) S.
```

Thus no Q/H multiplier belongs inside S. In particular, with private factors m_i=q_i/h_i, applying the same bijection to full masks gives Q=H*m_1*m_2*m_3.

### 2. Cheap First And Second Moments

Write B_i^(g)(r)=#{u in U_i : u=r mod g}. Every residue modulo h_i appears H/h_i times, so

```text
S_i = sum_(t<H) A_i(t mod h_i) = (H/h_i)|U_i|.
```

For a pair i,j, t modulo H is in bijection with pairs (s_i,s_j) modulo h_i,h_j satisfying s_i=s_j modulo g_ij. Therefore

```text
P_ij = sum_(t<H) A_i(t mod h_i) A_j(t mod h_j)
     = sum_(r<g_ij)
         (sum_(s_i=r mod g_ij) A_i(s_i))
         (sum_(s_j=r mod g_ij) A_j(s_j))
     = sum_(r<g_ij) B_i^(g_ij)(r) B_j^(g_ij)(r).
```

The last equality uses g_ij | h_i,h_j. It is the dot product of the **original native-mask** gcd histograms. It is not the full two-mask root count over H or L with another period multiplier. Such an extra multiplier would double-count here.

### 3. Modal Center And Signed Identity

For each attained histogram value v, let population_i(v)=#{s<h_i : A_i(s)=v}. Select c_i maximizing this population, breaking ties toward the smaller value. Equivalently minimize the number of nonzero entries in A_i-c over all constant c. This is a population-length mode, not the most frequent run record.

Put D_i=A_i-c_i, E_i={s<h_i:D_i(s)!=0}, and d_i=|E_i|. Subtraction can produce negative entries and entries with absolute value greater than one, although the native masks are Boolean. Define T=sum_(t<H) D_1 D_2 D_3, with the appropriate periodic arguments.

Expanding the three-factor product in T and rearranging gives exactly

```text
S = c_3 P_12 + c_2 P_13 + c_1 P_23
    - c_2 c_3 S_1 - c_1 c_3 S_2 - c_1 c_2 S_3
    + c_1 c_2 c_3 H + T.
```

The identity holds for any constants; modality is an execution heuristic. It is ordinary distributivity, not a probabilistic approximation. Using only pair/single terms without T is generally wrong.

If all histograms are constant, E_i is empty and S=H*c_1*c_2*c_3. This includes proper non-full masks: q=(12,50,105), h=(6,10,15), U_i=[0,h_i) gives H=30, Q=2100, c=(1,1,1), and C(Q)=30. A zero correction does not mean a zero result. If any one D_i is identically zero, T=0; this is broader than the all-mode case.

The mode minimizes d_i, not deviation magnitude, run count, join cost, or signed sum. For q=(3,3,3) and U_i={0}, modes give all d_i=1 and all K_ij=1. Changing only c_1 to 1 increases d_1 to 2 but makes K_12=K_13=0. This disproves a global optimal-centering interpretation. Equal-period simplification would of course solve that example too.

### 4. Exact Preflight K

Let F_i^(g)(r)=#{u in E_i:u=r mod g}. These are histograms of the Boolean support, not histograms of signed D_i weights. Then

```text
K_ij = sum_(r<g_ij) F_i^(g_ij)(r) F_j^(g_ij)(r)
     = #{(u,v) in E_i x E_j : u=v mod g_ij}.
```

Since every such pair corresponds to one t modulo H, K_ij is also the number of t for which both chosen deviations are nonzero. In particular,

```text
0 <= K_ij <= min(H, d_i*d_j,
                 d_i*(h_j/g_ij), d_j*(h_i/g_ij)).
```

Using d_i*d_j/g_ij as an exact count assumes an unproved uniformity condition; it can even be nonintegral. A signed dot product can cancel and is not a work counter.

Compute all three K values compactly. Choose the lexicographic key (K_ij, R_i'*R_j', i, j), i<j, with fixed input index order. Here R_i' counts maximal constant **nonzero weighted runs** of D_i in [0,h_i); it does not count support points. Adjacent nonzero runs with different weights may be merged for support histograms, but never for weighted accumulation.

### 5. CRT Correction With No Multiplicity Loss

For the chosen pair let g=g_ij, m=h_i, n=h_j, and n'=n/g. Given compatible u,v, define

```text
z = (((v-u)/g) * inverse(m/g modulo n')) mod n', if n'>1;
z = 0,                                                    if n'=1.
t = (u + m*z) mod H.
```

The division is exact, and m/g is coprime to n'. Precompute the inverse once for this orientation, not once per candidate. The n'=1 branch avoids asking an inverse routine to handle modulus one.

Each pair contributes D_i(u)*D_j(v)*D_k(t mod h_k). The map from compatible pairs to t is injective and onto the t where the first two factors are nonzero. Summing gives exactly T, including third-factor misses and cancellation. There is no factor H/lcm(h_i,h_j), since it equals one here. Signed division/remainder must implement mathematical integer semantics for negative v-u.

## Compact Construction And Enumeration Bound

### Histogram And Support Preparation

For a native run [a,b), length ell, its h-histogram is

```text
floor(ell/h) + 1[((s-a) mod h) < ell mod h].
```

Accumulate the constant parts and at most two cyclic jump positions per nonempty remainder arc. Initialize correctly across zero; coalesce equal positions and remove zero jumps. Sorting is necessary because reducing native endpoints modulo h can reorder them.

A linearly cut histogram has at most 2R_i+1 constant segments. Sum segment lengths by value to obtain the mode, using sorting or a deterministic ordered map for the claimed comparison bound, then subtract it. This also counts zero-valued histogram segments, which must not be silently omitted when selecting the mode. The weighted deviation representation satisfies R_i'<=2R_i+1. If there are M_i attained values, d_i<=h_i-ceil(h_i/M_i); this need not be small.

Apply the same interval-histogram procedure to the support runs modulo each needed g. Two coalesced histogram lists can be dotted by a simultaneous endpoint scan. Likewise compute all original-mask P_ij with compact gcd histograms. There are only constantly many such preparations, so the total is O(1+R log(R+1)) arithmetic/comparison operations and O(R+1) retained integer records. No h_i-, g_ij-, or H-sized arrays are needed.

### One Weighted Run Pair

Take [a,b) with constant nonzero weight alpha and [c,d) with constant nonzero weight beta. All endpoints are integer, runs are nonempty, and g>0.

If d-c>=g, every u in [a,b) has at least one compatible v. Enumerate u, then

```text
v_0 = c + ((u-c) mod g);
v = v_0, v_0+g, ... < d.
```

The number of visited u values is at most the number of emitted pairs.

If 0<d-c<g, [c,d) modulo g is one or two disjoint nonempty arcs [x,y) inside [0,g). Only u in repeated arcs [z*g+x,z*g+y) can have a partner. Jump directly to the overlapping blocks:

```text
z_first = floor((a-y)/g)+1
z_last  = floor((b-1-x)/g)
u in [max(a,z*g+x), min(b,z*g+y)) for z_first <= z <= z_last.
```

Every block in that range has a nonempty intersection with [a,b), and every generated u has exactly one compatible v because d-c<g. The two arcs cannot duplicate a u. Thus there is no loop across an empty residue gap, all a..b residues, all g residue classes, or all period copies with no output. Arithmetic initialization handles zero-output run pairs in constant work.

For each run pair the work is O(1+emitted pairs) before third-factor lookup. Disjoint weighted runs assign every support pair to exactly one run pair. Summing yields O(R_i'R_j'+K_ij) before lookups and O(R_i'R_j'+K_ij log(R_k'+2)) with binary search on sorted third-factor runs. The +2 makes the formula valid without awkward log(1)=0 cases; an empty third support should immediately skip the correction.

### Paid Resources And Admission

For the specified schedule:

```text
Preprocessing: O(1 + R log(R+1))
Correction:    O(R_i'R_j' + K_ij*(1 + log(R_k'+1)))
Total records: O(R+1), including input, sorted events, histograms,
               modes, support runs, weighted runs, and lookup index.
```

Enumeration uses cursors and a scalar accumulator. Storing the K pairs, CRT roots, a visited-root set, or every repeated arc would invalidate the production record bound. The probe below deliberately uses dense arrays and sets only as small independent oracles, not as a claimed compact implementation.

A sound post-preparation admission contract can cap K and M=R_i'R_j' separately, or use an explicit conservative score such as M+K*(1+ceil(log2(R_k'+1))) with declared constant charges for pair/CRT/lookup operations. The score is a defined logical work model, not exact machine work. K=0 permits T=0 with no run-pair loop at all. If there is a limit on total work, validation, sorting, histogram construction, and preflight itself must be separately capped or included. A rejection cannot promise to undo those already-paid costs.

Minimum K minimizes only the number of candidates among the three modal pair plans. With unequal K, it ignores run-pair and lookup costs; even equal K and equal run-pair counts can leave different third-factor lookup costs. Stable tie-breaking ensures reproducibility, not least-cost selection.

Suggested auditable counters:

| Counter | Meaning or bound |
| --- | --- |
| native_runs, event_records, histogram_segments | Preparation quantities, never mislabeled as instructions |
| mode_populations, support_cardinalities | Residue lengths, not numbers of segment records |
| weighted_runs, support_runs | Keep weight boundaries distinct from Boolean support boundaries |
| K_12, K_13, K_23; selected_pair | Exact preflight counts and deterministic choice |
| planned_run_pairs; visited_run_pairs | M before enumeration; actual visits can be zero after a shortcut |
| productive_u_visits | Counts u separately per run pair; at most K |
| nonempty_arc_blocks | At most productive_u_visits in the short-width case |
| compatible_pairs_emitted | Exactly K on a completed, non-shortcut correction |
| third_factor_lookups; nonzero_triple_hits | K lookups for the basic schedule; hits <= K |
| lookup_comparisons | Charge a per-lookup logarithmic upper bound plus containment check |
| correction_total; absolute_contribution_sum | Signed arithmetic result versus cancellation-free diagnostic |
| maximum_integer_bits; gcd/inverse calls | Big-integer and setup evidence, not RAM certification |
| refused_before_correction | No enumerated pairs after preflight rejects the correction |

For Boolean native input, 0<=A_i,c_i<=m_i and |D_i|<=m_i. Each of the seven signed baseline terms has magnitude at most Q, and sum_(t<H)|D_1D_2D_3|<=Q. Consequently the baseline/correction accumulators can be bounded by a fixed multiple of Q under ordinary sequential accumulation; final C(L)<=L. Raw CRT products and planning counters can exceed Q, although their bit widths remain O(log(Q+1)) for this fixed three-mask Boolean contract; arithmetic involving L also pays its width. These are operand-width bounds, not unit-cost guarantees. Preflight K<=H<=Q. Exact lcm, division, sorting comparisons, CRT multiply/reduce, and every weighted accumulation still pay their bit costs. Compute lcm as (a/gcd(a,b))*b with appropriate overflow discipline.

Retained integer records are not bytes or whole-process RAM. Parsing, runtime objects, sort workspace, allocator behavior, integer-arithmetic scratch, concurrent motifs, source storage, and output buffers need their own accounting. No bit-polynomial worst-case runtime or physical-memory measurement follows.

## Adversaries And Boundaries

### Requested Dense Complement Family

Let a,b,c>2 be pairwise coprime, q=(ab,ac,bc), and U_i=[1,q_i). Then h_i=q_i and H=Q=abc. The histograms are Boolean indicators, c_i=1, and D_i=-1 only at residue zero. Therefore every d_i=R_i'=1, every K_ij=1, and T=-1.

The pair moments are P_12=Q-c-b+1, P_13=Q-c-a+1, P_23=Q-b-a+1, and S_i=Q-Q/q_i. Substitution gives

```text
S = Q-a-b-c+2;  C(L)=(L/Q)*(Q-a-b-c+2).
```

All masks are dense and nonsingleton with different periods. Each histogram has two cyclic jumps, so a literal repeated-stream count is E=2(H/q_1+H/q_2+H/q_3)=2(a+b+c). Even merged distinct positions are Theta(a+b+c): they contain the largest stream and are bounded by the sum of the three stream sizes. Boundary-at-zero conventions change only constant offsets.

But let X_i={x<Q:x=0 mod q_i}. Their sizes are c,b,a; every pair intersection and the triple intersection has size one. Inclusion-exclusion gives the same answer immediately. This is an independently specified elementary control on the supplied masks, not a reimplementation importing the centered procedure. It defeats a claim that this family establishes advantage over elementary exact arithmetic, not a claim that every detail of the centered composition was previously published.

### Still Exponential With Three Different Periods

On the same q=(ab,ac,bc) pattern, take pairwise coprime a,b,c>=3 and U_i=[0,floor(q_i/2)). Now c_i=0, R_i'=1, and d_i=floor(q_i/2). For a pair sharing factor a, its two support histograms modulo a are at least floor(b/2) and floor(c/2) everywhere. Hence

```text
K_12 >= a*floor(b/2)*floor(c/2) >= H/9;
K_13,K_23 >= H/9 by symmetry; each K_ij <= H.
```

Thus the selected K is Theta(H) despite three runs, dense nonsingleton masks, and distinct periods. Families with growing binary-encoded a,b,c make this exponential in input bit length. The preflight can refuse such work exactly; it cannot turn it into polynomial enumeration. This is an analytical counterexample to an overclaim, not a lower bound on the counting problem.

### A Stronger Sparse-Join Comparator

Introduce three keys z_12,z_13,z_23 with domains [0,g_12),[0,g_13),[0,g_23). Map a support residue s of D_1 to (s mod g_12,s mod g_13), weighted by D_1(s); similarly map D_2 to keys (z_12,z_23) and D_3 to (z_13,z_23). Each mapping is injective because h_i is the lcm of its two key moduli. Each matching triangle of keys determines compatible s_1,s_2,s_3 and one t modulo H. Therefore the sum of products of the joined weights is exactly T.

The selected-pair schedule is a binary join followed by a third-relation lookup; K is its binary join size. This is an explicit reduction to established join machinery, not a claim that the publications implement the periodic run representation.

In particular, the zero-output, quadratic-binary-join example in Ngo et al., Example 2.2, embeds here: take q=(ab,ac,bc), pairwise coprime a,b,c>n, and on each two-key domain use {(0,r),(r,0):1<=r<=n}. Each support has 2n points, all pair joins have n^2+n candidates, but no triple can satisfy three "exactly one endpoint zero" constraints on a triangle. Choose the factors large enough that 2n<min(q_i)/2, so these are indeed mode-zero Boolean histograms. Native masks can be supplied as at most 2n singleton runs per mask. A published worst-case-optimal triangle join on the expanded supports has a subquadratic worst-case bound. This refutes universal optimality of minimum-K binary enumeration. [Ngo et al., full paper, Example 2.2 and Section 4](https://arxiv.org/pdf/1203.1952).

Expanding d_i points and building conventional indexes costs at least O(sum d_i) work/state, potentially far above O(R). Thus the explicit sparse join is a strong comparator in its matching representation, not an automatic replacement with the same compact-record contract. A compact-oracle version would need its own paid construction. No such implementation or benchmark was run here.

## Closest Published Work

Bounded search and direct passage inspection, not an exhaustive priority search. Publication dates below are paper dates, not search-engine crawl dates.

| Source and inspected passage | Established component and precise limit |
| --- | --- |
| [Gibson, A Density Chinese Remainder Theorem, Integers 14 (2014), A22](https://math.colgate.edu/~integers/o22/o22.pdf), Theorem 1; Section 3, equations (13)-(17); Section 4 | Exact pair compatibility and gcd-histogram dot product; interval decomposition into whole gcd blocks and remainder. Ordinary instantiations supply P_ij and support-pair K. The centered three-mask executor is not stated there. |
| [Kim and Glass, Perfect periodic scheduling for three basic cycles, Journal of Scheduling 17 (2014), 47-65](https://openaccess.city.ac.uk/16464/1/perfect%20periodic%20scheduling.pdf), Section 4 factor definitions and Section 4.1, Theorem 2/Lemma 5 | Their q_i/g_i0 equals h_i in this notation. Published private-factor removal for a scheduling-feasibility problem, not counting three prescribed masks. |
| [Ahmad et al., DBToaster: Higher-order Delta Processing for Dynamic, Frequently Fresh Views, PVLDB 5(10), 2012](https://arxiv.org/pdf/1207.0137), Sections 3.1-3.2, printed pp. 970-971 | Finite-support signed/rational multiplicities, product join weights, sum aggregation, and the delta product rule with its cross term. Repeated distributivity yields the centered expansion. This does not provide native modular compression, a modal choice, or the K preflight. A static residual need not be a temporal update to use the algebra. |
| [Baunsgaard and Boehm, AWARE: Workload-aware, Redundancy-exploiting Linear Algebra, PACMMOD 1(1), Article 2, 2023](https://mboehm7.github.io/resources/sigmod2023a.pdf), Section 3.1, pp. 2:4-2:5; Section 4.2, "Morphing," pp. 2:9-2:10 | Sparse Dictionary Coding selects the most-frequent default. Frame-of-reference/morphing separates a reference from a zero-default residual and uses aggregate correction in multiplication. This is particularly close precedent for default-plus-deviation computation. Its matrix input and numeric kernels are not the arbitrary-precision periodic-run CRT contract here. |
| [Abo Khamis, Ngo, and Rudra, Functional Aggregate Queries, arXiv:1504.04044v7 (2023 revision; initial preprint 2015)](https://arxiv.org/pdf/1504.04044v7), Definition 4.1 and Sections 8.1-8.3 | Sparse factor tables and representation-aware sum/product evaluation are established. The oracle discussion explicitly makes representation costs consequential. It does not justify replacing listing size by native run count without building and accounting for the necessary oracle. The version inspected is not silently dated 2015. |
| [Ngo, Porat, Re, and Rudra, Worst-case Optimal Join Algorithms, PODS 2012 full-paper preprint](https://arxiv.org/pdf/1203.1952), Section 2/Example 2.2, Section 4, and triangle specialization in Section 7.1 | Published sparse multiway joins expose the binary-pair plan's worst-case limitation. The residue-key embedding above is this review's ordinary instantiation; the paper does not supply this compact periodic representation or its budget contract. |

DBToaster's publisher/mirror PDF text retrieval was unreliable; the arXiv full text supplied the actually inspected model and delta rules. AWARE is a stronger match for the mode/default step than a generic appeal to "sparse methods." No claim of the earliest appearance of centering is needed to establish this precedent.

The fixed-dimensional lattice comparator in Shared-Residue-Review remains relevant: for each run triple count integer (x,k_1,k_2,k_3) with 0<=x<Q and a_i<=x-q_i*k_i<=b_i-1. This is a four-dimensional bounded polytope with one tuple per satisfying root, so the previously documented fixed-dimension method avoids numeric-period enumeration at a cost polynomial in bit width per run triple. This review did not execute that comparator. Its [Barvinok-Pommersheim source](https://library.slmath.org/books/Book38/files/barvinok.pdf) timed out on fresh retrieval; theorem-level source inspection is inherited explicitly from the requested prior review, not claimed afresh here.

No inspected source states all of: these native Boolean runs, shared-factor reduction, population-mode centering, compact exact K computation, productive-u interval enumeration, and deterministic refusal before correction enumeration. That bounded negative search result is neither a priority proof nor license to promote every inherited step as new.

## Bugs And Verification Obligations

These are conditional implementation hazards, not findings in unseen lead code.

1. **Wrong scaling:** omit L/Q or insert an extra H/g or Q/H factor. Unit/coprime/private-factor fixtures distinguish these errors.
2. **Wrong mode:** count segments instead of residue lengths, ignore zero segments, or break ties inconsistently.
3. **Unsigned or Boolean deviations:** clamp negative D, replace nonzero D by one, or assume |D|<=1. The signed-weight fixture below has deviations from -3 to +4.
4. **Wrong K:** use signed weights, d_i*d_j/g, or number of support runs instead of the exact support-histogram dot product.
5. **Hidden empty scanning:** iterate every u, every residue class, or period blocks with no productive u in the short-width branch. Constant-record storage alone does not repair that work bug.
6. **Endpoint errors:** confuse d with width d-c, mishandle width exactly g, make the arc endpoint inclusive, duplicate wrapped arcs, or mishandle negative floor division in the block bounds.
7. **Weighted coalescing errors:** merge adjacent different-weight deviations because their supports touch, or double-count overlapping native intervals.
8. **CRT corner cases:** invert the unreduced modulus, fail the n'=1 branch, mishandle negative differences, or recompute inversion for every emitted pair without charging for it.
9. **Missing third lookup:** treating every compatible pair as a nonzero triple is wrong. Cancellation and misses are legitimate even with K>0.
10. **Incorrect zero shortcut:** K=0 or constant histograms eliminate T, not the baseline. Still validate all masks.
11. **Admission mismatch:** check K only after producing pairs, claim preprocessing is free, or label a logical score as a deadline/RAM bound.
12. **Scope creep:** generalize the pair-lcm identity beyond three masks, claim a root-membership output of period H, or turn a scalar symbolic result into a delivered whole-graph output claim.

## Independent Checks

Executed with python3.11 -B, standard library only, seed 20260921. The interval enumeration oracle directly tests (u-v)%g==0. The scalar oracle scans roots and native mask membership; it does not use CRT or the centered expression. The formula side builds dense small histograms independently. Every pair orientation is checked, not just the selected minimum. The snippet imports no repository code and writes no files.

- 7,776 interval-pair cases: g in 1..6, every nonempty interval with endpoints in 0..8, and all ordered interval pairs. Checks output equality, no duplicates, and productive-u charging.
- 200 seeded native-mask cases: ordered periods in 1..12, random Boolean subsets, L/Q in 1..3. Checks reduced gcd/lcm, original pair moments, signed expansion, exact K, pair-to-t uniqueness, streamed T, and root-oracle agreement.
- Nine named/structured cases: unit, empty, coprime, all-mode, tie, incompatible supports, signed-weighted, and two dense-complement families.

| Case | C(L) | c | d | (K_12,K_13,K_23) | T |
| --- | ---: | --- | --- | --- | ---: |
| Unit periods, L=3 | 3 | (1,1,1) | (0,0,0) | (0,0,0) | 0 |
| Coprime (3,5,7), L=210 | 24 | (2,2,3) | (0,0,0) | (0,0,0) | 0 |
| Proper all-mode (12,50,105), L=2100 | 30 | (1,1,1) | (0,0,0) | (0,0,0) | 0 |
| Tie (2,2,2), singleton zero masks | 1 | (0,0,0) | (1,1,1) | (1,1,1) | 1 |
| Incompatible (4,6,8), masks {0},{1},{0} | 0 | (0,0,0) | (1,1,1) | (0,1,0) | 0 |
| Signed-weighted (12,50,105) | 246 | (1,2,3) | (2,2,2) | (2,2,2) | 6 |
| Dense complement a,b,c=(3,4,5) | 50 | (1,1,1) | (1,1,1) | (1,1,1) | -1 |
| Dense complement a,b,c=(5,7,11) | 364 | (1,1,1) | (1,1,1) | (1,1,1) | -1 |

All assertions passed. These checks do not test a compact histogram constructor, budget refusal, measured memory, sorting complexity, lead tests, or lead code. Those remain verification obligations, not inferred successes.

### Self-Contained Executed Snippet

Run the following Python block with python3.11 -B. Dense data structures are intentional independent small-case oracles.

```python
from collections import Counter
from itertools import combinations, product
from math import gcd, lcm, prod
from random import Random

PAIRS = ((0, 1), (0, 2), (1, 2))

def make_weighted_value_runs(values):
    runs = []
    start = 0
    while start < len(values):
        stop = start + 1
        while stop < len(values) and values[stop] == values[start]:
            stop += 1
        runs.append((start, stop, values[start]))
        start = stop
    return runs

def iterate_productive_run_values(a, b, c, d, g):
    if d - c >= g:
        yield from range(a, b)
        return
    start, width = c % g, d - c
    arcs = [(start, min(g, start + width))]
    if start + width > g:
        arcs.append((0, start + width - g))
    for left, right in arcs:
        first = (a - right) // g + 1
        last = (b - 1 - left) // g
        for block in range(first, last + 1):
            yield from range(max(a, block*g + left),
                             min(b, block*g + right))

def stream_compatible_run_pairs(a, b, c, d, g):
    for u in iterate_productive_run_values(a, b, c, d, g):
        first = c + (u - c) % g
        assert first < d
        for v in range(first, d, g):
            yield u, v

def reconstruct_compatible_residue_pair(u, v, m, n):
    g = gcd(m, n)
    assert (v - u) % g == 0
    reduced = n // g
    offset = 0 if reduced == 1 else (
        ((v-u)//g) * pow(m//g, -1, reduced)) % reduced
    return (u + m*offset) % lcm(m, n)

def check_native_mask_case(q, masks, scale=1):
    h = tuple(lcm(*(gcd(q[i], q[j]) for j in range(3) if i != j))
              for i in range(3))
    H, Q = lcm(*h), lcm(*q)
    hist = [[sum(u % h[i] == s for u in masks[i])
             for s in range(h[i])] for i in range(3)]
    center = []
    delta = []
    runs = []
    for values in hist:
        frequencies = Counter(values)
        baseline = min(frequencies, key=lambda v: (-frequencies[v], v))
        center.append(baseline)
        delta.append([v-baseline for v in values])
        runs.append([r for r in make_weighted_value_runs(delta[-1]) if r[2]])
    total = sum(prod(hist[i][t % h[i]] for i in range(3)) for t in range(H))
    correction = sum(prod(delta[i][t % h[i]] for i in range(3))
                     for t in range(H))
    singles = [H//h[i]*len(masks[i]) for i in range(3)]
    pairs, counts = {}, {}
    for i, j in PAIRS:
        k = 3-i-j
        g = gcd(q[i], q[j])
        assert gcd(h[i], h[j]) == g and lcm(h[i], h[j]) == H
        native_i = Counter(u % g for u in masks[i])
        native_j = Counter(u % g for u in masks[j])
        pairs[i, j] = sum(native_i[r]*native_j[r] for r in range(g))
        assert pairs[i, j] == sum(hist[i][t % h[i]]*hist[j][t % h[j]]
                                 for t in range(H))
        support_i = Counter(u % g for u, w in enumerate(delta[i]) if w)
        support_j = Counter(v % g for v, w in enumerate(delta[j]) if w)
        counts[i, j] = sum(support_i[r]*support_j[r] for r in range(g))
        seen, weighted = set(), 0
        for a, b, wi in runs[i]:
            for c, d, wj in runs[j]:
                for u, v in stream_compatible_run_pairs(a, b, c, d, g):
                    t = reconstruct_compatible_residue_pair(u, v, h[i], h[j])
                    assert t not in seen
                    seen.add(t)
                    weighted += wi*wj*delta[k][t % h[k]]
        assert len(seen) == counts[i, j]
        assert weighted == correction
    c1, c2, c3 = center
    rebuilt = (c3*pairs[0, 1] + c2*pairs[0, 2] + c1*pairs[1, 2]
               - c2*c3*singles[0] - c1*c3*singles[1] - c1*c2*singles[2]
               + c1*c2*c3*H + correction)
    direct = sum(all(x % q[i] in masks[i] for i in range(3))
                 for x in range(scale*Q))
    assert rebuilt == total and direct == scale*total
    chosen = min(PAIRS, key=lambda ij: (counts[ij],
                 len(runs[ij[0]])*len(runs[ij[1]]), ij))
    return (direct, tuple(center),
            tuple(sum(w != 0 for w in values) for values in delta),
            tuple(counts[ij] for ij in PAIRS), correction, chosen,
            (min(map(min, delta)), max(map(max, delta))))

intervals = list(combinations(range(9), 2))
interval_checks = 0
for g, (a, b), (c, d) in product(range(1, 7), intervals, intervals):
    actual = list(stream_compatible_run_pairs(a, b, c, d, g))
    expected = {(u, v) for u in range(a, b) for v in range(c, d)
                if (u-v) % g == 0}
    assert len(actual) == len(expected) and set(actual) == expected
    assert len(set(u for u, v in actual)) <= len(actual)
    interval_checks += 1
print("interval-pair checks:", interval_checks)

rng = Random(20260921)
for _ in range(200):
    q = tuple(rng.randrange(1, 13) for _ in range(3))
    masks = tuple({u for u in range(qi) if rng.randrange(2)} for qi in q)
    check_native_mask_case(q, masks, rng.randrange(1, 4))
print("seeded native-mask checks: 200")

fixtures = [
    ("unit", (1, 1, 1), ({0}, {0}, {0}), 3),
    ("empty", (2, 3, 4), (set(), {0, 1}, {1, 3}), 2),
    ("coprime", (3, 5, 7), ({0, 2}, {1, 2}, {0, 2, 4}), 2),
    ("all-mode", (12, 50, 105),
     (set(range(6)), set(range(10)), set(range(15))), 1),
    ("tie", (2, 2, 2), ({0}, {0}, {0}), 1),
    ("incompatible", (4, 6, 8), ({0}, {1}, {0}), 1),
]
target = ([0, 1, 1, 1, 1, 2], [0, 2, 2, 2, 2, 2, 2, 2, 2, 5],
          [0, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 7])
weighted_masks = tuple({s + len(values)*z
                        for s, value in enumerate(values) for z in range(value)}
                       for values in target)
fixtures.append(("signed-weighted", (12, 50, 105), weighted_masks, 1))
for a, b, c in ((3, 4, 5), (5, 7, 11)):
    q = (a*b, a*c, b*c)
    masks = tuple(set(range(1, qi)) for qi in q)
    result = check_native_mask_case(q, masks)
    assert result[0] == a*b*c-a-b-c+2
    assert result[3] == (1, 1, 1) and result[4] == -1
    print("dense-complement", (a, b, c), result)
for name, q, masks, scale in fixtures:
    result = check_native_mask_case(q, masks, scale)
    print(name, result)
print("all independent checks passed")
```

## Final Synthesis And Claim Ledger

**Accept:** the exact algebra, the three-mask CRT correspondence, modal support minimization, compact K preflight, and the output-sensitive run-pair enumeration proof. Use the qualified arithmetic and retained-record bounds above.

**Narrow:** "exact work admission" to exact candidate counts plus a declared conservative work schedule; "best pair" to minimum K among three fixed modal plans; "weighted" to signed integer deviations of Boolean native masks unless a separate weighted-input contract is explicitly added.

**Reject:** novelty of centering/distributivity/CRT/sparse join as primitives; a separation from elementary counting based solely on the complement family; run-linear or bit-polynomial worst-case time; universal join optimality; and whole-physical-RAM or graph-output conclusions from this scalar experiment.

The strongest contribution statement currently supportable is:

> For three native run-encoded periodic Boolean masks, the composition evaluates the constant, first-, and second-order centered terms through compact residue aggregation, computes exact compatible residual-pair counts before enumeration, and streams one chosen residual correction with a proved candidate-sensitive arithmetic bound and run-sized retained records.

That is a mathematically defensible **composition contract**, not yet a demonstrated new published algorithm. The particular compressed-oracle construction and its paid guarantee are where a possible contribution remains; the inspected predecessors do not automatically erase it. Conversely, "another engine can copy it" supplies no priority evidence.

Next evidence should distinguish the following categories without editing this review's scope:

| Category | Current status |
| --- | --- |
| Published components | CRT/histogram counting; shared-factor removal; mode/default residuals; signed delta aggregation; sparse multiway joins |
| Ordinary instantiations | Centered polynomial identity; residue-key triangle join; scalar periodic grouping |
| Constructed controls | Complement inclusion-exclusion on the requested family; representation-matched selectors and lattice reductions, with their costs explicitly paid |
| Candidate-importing controls | Any comparator receiving the new compact support-enumeration procedure must mark that import; agreement is not historical priority |
| Unresolved contribution | Whether the complete compact periodic preparation/preflight/enumeration contract is already documented elsewhere, and whether it yields a useful nontrivial regime beyond current elementary controls |

A stronger positive case needs a family surviving complement/same-period/singleton reductions, matched comparison against expanded sparse joins and fixed-dimensional counting, and either a nontrivial theoretical separation under the same representation/resource contract or useful end-to-end evidence. No such empirical claim is made here. The lead's TDD reference and manuscript remain independently owned and unreviewed by this sidecar.

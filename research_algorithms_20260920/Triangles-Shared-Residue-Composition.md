# Shared-Residue Triangle Composition Without Global Period Expansion

Date: 2026-09-20. Active A07 research. Exact counting primitive and graph integration implemented; seventeen focused tests pass, with 51 across the masked/hybrid, refinement, shared-residue and elementary-control suites. The [mathematical/prior-art review](Triangles-Shared-Residue-Review.md) and separate [implementation review](Triangles-Shared-Residue-Code-Review.md) are complete. The latter found one source-validation mismatch, corrected by the lead with a red/green regression. This is a research manuscript, not a global-novelty or physical-memory claim.

## Objective And Chosen Design

Periodic refinement needs a common mask period and can enlarge the prepared graph. Instead, retain every edge class in its own native period and compose local triangle contributions directly. No common-period vertex table is required.

For one closing class triple, its legal root coordinates satisfy three periodic interval predicates. The conventional control merges the predicates' repeated boundary streams over their least common multiple Q. The proposed primitive first marginalizes each predicate onto only the residue moduli it shares with the other two. An event sweep then works over a potentially smaller shared period H. A graph wrapper sums these exact local contributions, counts degrees separately, and provides a blockwise complete-output evaluator without retaining one term per class triple.

Other considered routes are global refinement with exact convolution and direct root scanning. They remain controls: this design does not assume that period expansion is the only competent alternative. Generalized Chinese remainder arithmetic, range histograms and event sweeps have prior art; the candidate contribution needs a precise comparison of their composition, not a new name for them.

## Primitive Contract

Let q1,q2,q3 be positive integers dividing L. Ui is a Boolean set of integer residues represented as disjoint half-open intervals in [0,qi). Define

```text
gij = gcd(qi,qj)
hi  = lcm(gij,gik), for distinct i,j,k
H   = lcm(g12,g13,g23)
Q   = lcm(q1,q2,q3)
Ai(t) = number of r in Ui with r = t (mod hi).

proposed root count = (L/Q) * sum_(t=0..H-1)
                              A1(t mod h1) A2(t mod h2) A3(t mod h3).
```

Each Ai is built as a runwise integer histogram: a native interval of length n contributes floor(n/hi) everywhere and one circular remainder interval of length n mod hi. It is not a Boolean support histogram. A three-stream boundary sweep integrates the product without allocating H cells. Coincident boundaries and circular changes at zero require exact handling. A supplied logical event budget may refuse before the sweep; that budget is not process RSS.

## Verification-First Work Plan

1. Add `experiments/test_shared_residue_triangles.py` with failing missing-primitive assertions, independent integer-root enumeration, coprime huge-period examples, shared-prime-power cases, histogram conservation, empty/full masks and invalid-input checks.
2. Implement `experiments/probe_shared_residue_triangles.py`: native validation, compact residue histograms, periodic boundary cursors, exact product integral, and paid event admission. Do not allocate Q or H arrays.
3. Extend tests before implementing the graph wrapper: independent expanded edges, all local counts/degrees, varying output block sizes, shifts/wraparound, and multiple incident base triangles with incompatible local periods.
4. Prove the CRT reduction, event-work comparison and graph-level composition; document preparation, output and arithmetic costs, adversarial failure of naive density multiplication, and closest primary art.
5. Integrate the separately authored shared-residue review. It does not review code written after its mathematical task snapshot unless explicitly stated.

The prior goal turn was progress, not a wait. The full seven-family innovation goal remains active; this manuscript does not redefine success as another passing small probe.

## What Changes From Periodic Refinement

The previous representation uses a common period p, F_p=E*p/L refined classes and bp local-counter rows. When three native mask periods are nearly coprime, p may already be their product, even if each mask is described by one interval. The new procedure does not construct that refined graph or its residue-row table. It keeps native masks, joins displacement classes, and composes exact root counts.

```text
Three native masks: q1, q2, q3
               |
               v
Pairwise gcds -> shared marginal moduli h1,h2,h3
               |
               v
Compact multiplicity histograms, not Boolean masks
               |
               v
Three lazy boundary streams over shared period H
               |
               v
Exact root count -> sum across closing class triples
```

Full local results follow a separate exact path: regenerate incident class terms into a fixed-size output block. This avoids storing a table of all class triples but can substantially increase output computation. It is a time-space option, not free complete output.

## The Shared-Residue Theorem

For distinct i,j,k let `gij=gcd(qi,qj)`, `hi=lcm(gij,gik)`, `H=lcm(g12,g13,g23)` and `Q=lcm(q1,q2,q3)`. Each hi divides qi. For `t in Z_hi`, let Ai(t) count the actual residues in Ui congruent to t modulo hi. Then the formula in the primitive contract is exact.

**Proof by compatible residue tuples.** A root g modulo Q gives a tuple `(r1,r2,r3)` with `ri=g mod qi`. Conversely, such a tuple comes from a unique g modulo Q exactly when every pair agrees modulo gij. To see sufficiency without assuming coprime qi, consider each prime separately, select a qi with its largest prime-power exponent, and take that residue modulo the largest power. Pairwise compatibility makes every other residue its restriction. The ordinary coprime CRT combines these prime-power choices uniquely modulo Q.

All pairwise compatibility constraints on ri depend only on ri modulo hi. For a prime whose three exponents are ordered `e1<=e2<=e3`, the exponents in the three h values are `e1,e2,e2`, and H has exponent e2. Consequently the compatible tuples of h-residues correspond bijectively to t modulo H. This also gives `gcd(hi,hj)=gij`.

Fixing t fixes those shared h-residues. There are independently Ai(t mod hi) choices of ri in each Ui; every combination remains pairwise compatible and gives one root modulo Q. Multiply those counts, sum over t, and multiply by L/Q for the repeated Q-periods in [0,L). This proves the formula. No independence is assumed before conditioning on the shared residues.

Define `mi=qi/hi`. The same prime-exponent argument gives the useful identity

```text
Q = H*m1*m2*m3.
```

The mi remove unshared *tails of prime-power exponents*, not necessarily primes absent from every other period. For example, (12,18,20) yields h=(12,6,4), m=(1,3,5), H=12 and Q=180. No integer factorization is needed by the algorithm: gcd/lcm computations suffice.

The theorem is a specialization of generalized CRT and weighted marginalization. Its validity does not establish historical priority for the resulting algorithm. The completed review identifies published removal of exactly these private period factors, not merely vaguely related use of CRT; see the primary-art ledger below.

**Scalar aggregation is not a local-answer representation.** At q=(2,3,5), with all masks selecting only residue zero, H=1 and the scalar numerator is one. The actual satisfying roots have period Q=30, not H=1. Repeating the marginal product as a root indicator would incorrectly make every vertex participate. The exporter therefore evaluates the original three predicates; `test_histogram_not_local_indicator` checks a first-base local vector of one followed by 29 zeros. Also, h_i cannot all be replaced by the triple gcd, and H need not be coprime to Q/H. Neither shortcut is used in the proof or implementation.

## Compact Histogram Construction

For one native interval [a,a+n) and modulus h dividing q, write `n=u*h+r`, `0<=r<h`. Every residue receives u occurrences; the remaining r occurrences occupy the circular interval starting at a mod h. Accumulate the constant parts and emit the two cyclic boundary changes of each nonempty remainder. Sort the boundary positions, combine simultaneous changes, and form maximal constant runs including zero-count ranges.

The histogram has O(R_i+1) runs, independent of q and h, where R_i is the number of native intervals. It may have counts greater than one. Treating it as Boolean membership loses valid roots; the retained q=12/modulus=3 fixture has histogram values (2,3,3), not a three-element support indicator.

A native interval may wrap after reduction. The implementation splits that remainder at h, accounting for the change at zero. Adjacent equal-valued runs are coalesced. A constant histogram has no periodic change events even if its native period is enormous.

## Event-Work Comparison Theorem

This comparison is against a specified control: a competent raw periodic boundary merge over one Q-cycle, followed by multiplication by L/Q. It is not a lower bound for all CRT-aware, support-aware or temporal-join algorithms.

Let c_i be the number of nonzero cyclic changes of the native Boolean mask fi on Z_qi, and d_i the number of nonzero cyclic changes of its histogram Ai on Z_hi. The histogram's discrete cyclic derivative is the residue fold of the mask derivative:

```text
Ai(t)-Ai(t-1) = sum_(r mod hi=t) [fi(r)-fi(r-1)].
```

Folding can merge or cancel change positions but cannot introduce more nonzero positions, so `d_i<=c_i`. The raw and shared full-cycle event counts are respectively

```text
K_raw    = sum_i (Q/qi)*c_i
K_shared = sum_i (H/hi)*d_i.
```

Because `(Q/qi)/(H/hi) = product_(j!=i) mj`, each shared contribution is no larger than the corresponding raw contribution. Thus **K_shared<=K_raw**. Constant predicates have zero cyclic changes and are handled without empty cycle loops.

For a sweep whose initial values at zero are already known, an actual stream emits `(H/hi)*d_i-w_i` events, where w_i is one if the histogram changes across zero and zero otherwise. The reference plans this exact number before starting the sweep. At most three zero-position events separate that count from the cyclic count. Simultaneous events from different streams are individually applied; their zero-length intermediate segments contribute zero, so no ordering error occurs.

The bound concerns boundary work, not total elapsed time. Histogram creation, sorting, gcd/lcm, native source access and integer arithmetic remain paid. A raw control can have less setup overhead when no marginalization is useful.

### Exponential Event Work With Six Histogram Runs

The independent review supplies the following adversary. For even N, choose q=(2,2N,2N), L=2N and masks [0,1), [0,N), [0,N). There are three input runs and six retained histogram runs. Nevertheless H=Q=2N and the current implementation processes 2N+1 events, versus 2N+4 cyclic stream events. The exact answer is N/2.

The lead executed the existing implementation on the following cases; these are event counters, not timings or an independently implemented oracle:

| N | H=Q | Retained histogram runs | Actual processed events | Exact roots |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 16 | 6 | 17 | 4 |
| 64 | 128 | 6 | 129 | 32 |
| 1,024 | 2,048 | 6 | 2,049 | 512 |
| 8,192 | 16,384 | 6 | 16,385 | 4,096 |

Taking N=2^k makes event work exponential in input bit length. Intersecting the two equal-period masks first reduces the problem to ordinary two-mask gcd-histogram counting, which does not repeat those boundaries. Thus the example refutes a time claim about this executor, not a complexity lower bound for the counting problem. Distinct-period singleton masks can also lose to direct CRT; eliminating identical inputs is not a universal repair.

## Reference Procedures

```text
COUNT_ROOTS(L, q[3], U[3], event_budget):
    validate all native periods and canonical masks
    compute pairwise gcds, h[3], H and Q
    form compact multiplicity histograms A[3]
    compute each stream's exact event count
    return zero immediately if a native mask is empty
    refuse if the proposed sweep exceeds event_budget
    merge three lazy periodic change streams
    integrate gap_length * A1 * A2 * A3
    return (L/Q) * integral

COUNT_GRAPH(native source):
    normalize and index displacement classes
    enumerate ordered base triangles
    join the cheapest pair of displacement sets
    check exact displacement closure modulo L
    align the middle mask by the first displacement
    add COUNT_ROOTS for each closing class triple
    retain the source, total and bounded counters, not the term list

EXPORT_ROWS(source, block_size):
    for each original base and coordinate block:
        count incident edges into degree[block_size]
        regenerate incident closing class triples
        evaluate each triple's three native predicates into local[]
        emit every (degree, local_count) row in original-ID order
    check full-stream degree and triangle conservation
```

The finite code is [probe_shared_residue_triangles.py](experiments/probe_shared_residue_triangles.py). It uses existing validated interval and oriented displacement helpers. It does not use a floating FFT or create an H/Q-sized array.

## Graph-Level Correctness

The source remains a simple undirected cross-base graph with vertices `(a,g)` for `g in Z_L`. Class `(a,c,d)` has its own native period q dividing L and mask U; it contains the edge `(a,g)--(c,g+d mod L)` precisely when g mod q belongs to U. Empty classes are removed. Canonical class keys, mask periods, bounds and integer types are validated before processing.

Every actual triangle has three distinct ordered bases `a<c<z` and a unique displacement triple `(x,y,d)` with `x+y=d mod L`. Its root predicate is

```text
f_ac(g) * f_cz(g+x) * f_az(g).
```

Rotating the middle mask by -x in its own native period gives exactly the primitive's three root masks; q divides L, so this rotation agrees with graph-coordinate wraparound. The base traversal and cheapest-pair join enumerate each class triple once. Therefore summing its exact root count counts each triangle once globally.

For a local row at coordinate u, the root is u on the first base, u-x on the second, and u-d on the third. Evaluating the same three predicates gives exactly that triangle's local contribution at the requested vertex. Local contributions add across all incident base triangles even when their combined period has a huge least common multiple. No global period table is necessary.

Degrees are computed directly from edge classes, once at each endpoint. They are not summed from each triangle subproblem: a K4 fixture would otherwise overcount them. The full output satisfies `sum(degree)=2E` and `sum(local)=3T`; the iterator checks these after complete consumption. Isolates are included. Local clustering follows as `2*local/(degree*(degree-1))`, with an explicit zero convention below degree two. Exact numeric parity with a particular GDS floating format is not claimed.

## Memory, Work And Lifecycle

For one primitive with R total native runs, the reference uses O(R+1) retained histogram/normalization state and at most three pending heap events. Arithmetic values may be large; in bit complexity, charge gcd/lcm and arithmetic on O(log L)-bit primitive counts. The integral is at most Q: each Ai is at most qi/hi and `H*product(qi/hi)=Q`. Global triangle accumulation needs wider counters under a fixed-width implementation.

Its work is O(R log(R+1)+K) arithmetic operations after reading the description, where K is the planned actual event count. There is no loop proportional to Q or H unless the number of actual changing boundary events is itself that large. This is not a polynomial-time claim in the binary size of arbitrary periods: K can be enormous relative to log L. A logical event budget limits only these processed events, not source parsing, class joins, arithmetic bit cost or wall-clock time.

For the graph wrapper, let M and R count the native source classes and runs, W_B the paid base-neighborhood probes, C the paid displacement pair probes and J the closing class triples. Primitive construction and sweep costs are summed separately over those J triples. No J-entry term list is retained; auxiliary state beyond the source scales with the largest active primitive and the selected output block. Source dictionaries and indexes retain O(M+R) description state; there is no per-original-vertex allocation.

For output, let W_a be the base scan work for one block of base a, C_a its incident class pair probes, J_a its incident closing terms and M_a its incident source classes. With B rows per block, a disclosed upper work shape is

```text
sum_a ceil(L/B)*(W_a + C_a + M)
  + L*sum_a [M_a + 3*J_a]*O(log(R+1))
  + unavoidable bL row delivery.
```

The M term includes the present full class-index scan per block before filtering incident classes. These repeated reads can dominate. A disk term spool or better incident index would be separate storage/work options, not invisible optimizations. The reference keeps two B-sized output arrays; a caller materializing the iterator defeats that output-memory property.

Native source construction, proving eligibility of an arbitrary CSV/Neo4j import, retaining source versions, changing period encodings, snapshot overlap and recovery are still outside this finite executor. The code does not impose a process memory cap, implement external sorting, or validate physical 4 GB execution. Complete output over a symbolic trillion/exa-scale graph has not been delivered or timed.

## Executed Cases

The raw-cycle counts below are **derived exactly from source metadata**, not measured execution of the raw baseline. Shared events are actually processed by the candidate. These are logical experiments, not Neo4j/GDS performance comparisons.

| Native periods | Q | H | Exact root count per Q | Raw cyclic boundaries | Shared cyclic boundaries | Actual shared events |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 101,103,107; interval widths 20,30,40 | 1,113,121 | 1 | 24,000 | 64,462 | 0 | 0 |
| 12,18,20; mixed runs | 180 | 12 | 20 | 116 | 17 | 16 |
| 1,000,003;1,000,033;1,000,037; widths 123,456,789 | 1,000,073,001,431,003,663 | 1 | 44,253,432 | 6,000,292,002,862 | 0 | 0 |
| 101,103,10,403; two singleton masks | 10,403 | 10,403 | 1 | 410 | 410 | 407 |
| Three periods 10^12; upper-half masks | 10^12 | 10^12 | 500,000,000,000 | 6 | 6 | 3 |

The first and third rows use pairwise coprime periods, so all histograms are scalar cardinalities. That special case is direct CRT, not a new discovery. The last row demonstrates why a large H does not force a large array or dense scan. The fourth row is also an adversary: two singleton predicates determine a CRT solution that a support-aware control can test directly, avoiding our 407-event sweep.

## Falsifiers And Corrected Temptations

1. **Multiply all densities.** False with shared factors. At L=4, two period-two masks selecting opposite parity and one always-true mask have zero legal roots; multiplying their densities predicts one. Conditioning on shared residues repairs this.
2. **Discard histogram multiplicity.** False because multiple native residues can share one retained residue. The explicit (2,3,3) histogram is preserved as a regression. A hand-calculated fixture was corrected before implementation; it was not used to force an incorrect implementation.
3. **A small source guarantees a short run.** False: event repeats can be huge in binary period size. The exact event count is planned and can be refused, but class-join work and source preparation remain separate budgets.
4. **A fast global count makes local export fast.** False: complete local output is at least bL rows, and this low-retained-state exporter regenerates terms. The symbolic huge-source example consumes only the first eight rows; it is not a complete-output benchmark.
5. **No more events means universally faster.** False: setup and support-aware CRT solvers can win. The comparison theorem is intentionally against the specified raw boundary-merge control, not all algorithms.
6. **Native periods are free to discover.** False: an arbitrary graph may have no useful native mask representation, and proving an importer preserved it is part of preparation. No production graph's eligibility was measured here.

## Verification Receipt

Eight tests first failed because the primitive did not exist. After implementation they passed. Two additional primitive edge cases and five graph-level tests were then added; the five graph tests failed on the missing graph wrapper. After implementing the wrapper/exporter, fifteen focused tests passed. A sixteenth existing-behavior regression then checked that the scalar histogram is not used as a local root indicator; this test passed immediately and is not presented as a missing-feature red phase. The later implementation review prompted a seventeenth regression, detailed below. All seventeen focused tests and all 51 combined triangle/control tests pass:

```sh
python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p test_shared_residue_triangles.py
```

- Exhausted all 512 Boolean mask combinations at periods (2,3,4), comparing with direct integer-root enumeration.
- Checked 700 deterministic-seed random period/mask triples, including repeated complete periods, against that direct oracle. Exact planned/processed event counts and the cyclic-boundary comparison were also asserted.
- Checked 60 new native graph snapshots against independently expanded adjacency, every degree/local count and the global total. These are distinct from the preceding refinement suite's 120 graphs.
- Checked shifted closure at three output block sizes, isolates, degree-once K4 accounting, invalid native inputs and logical event-budget refusal.
- A three-class huge native graph has no retained triangle-term list, exact total 44,253,432 and a validated first-eight-row prefix. Its full output was not materialized.
- A fully shared period of 10^12 with three half-period masks completes with six histogram runs and three processed changes, returning the exact half-period count.
- The independent reviewer separately checked 27,000 exhaustive mask cases, 1,000 random root cases and 53,205 interval/divisor histogram cases using fresh mathematical probes with no experiment imports. Those results support the identity and histogram construction, not the lead's code, graph composition, event-budget implementation or physical memory.

These tests are finite falsification evidence, not formal machine-checked proofs or physical resource measurements. Independent code review is a separate receipt below, not an inference from the lead's passing suite.

### Separate Implementation Review And Fix

The completed [implementation review](Triangles-Shared-Residue-Code-Review.md) inspected the original primitive, graph normalization, closure, aggregation and exporter plus their four imported helpers. It independently checked 4,135 finite graph snapshots, 8,309 full exports and 75,001 rows, along with separate shift/closure/scalar/admission cases. No numerical or stated logical-resource mismatch was found in that scope. These counts are the reviewer's receipt, not a second lead replay or an exhaustive proof.

One P3 validation discrepancy was found: Python considers `(False,True,False)` and floating-point aliases equal to `(0,1,0)`. The period-map key equality test therefore accepted noninteger metadata keys while validating only the canonical key from the class map. `test_period_mapping_aliases` reproduced twelve failures: bool/float in each of three components, with an empty and nonempty mask. The lead added an explicit integer check over the actual period-map keys before equality matching; the full 51-test suite then passed. This adds O(M) validation reads and no expanded state. It changes rejection behavior, not arithmetic for canonical sources.

The reviewed source hash remains in the review; the post-fix `probe_shared_residue_triangles.py` SHA-256 is `0700a97cf86ac00c7cd64facf5824fb93df13f5fb9c75782b5f9d942384c1baf`. The reviewer did not re-review this two-line fix or the separately written elementary comparator. Its remaining principal implementation risk is paid high-fanout source/class work and complete-output regeneration on a realistically sized source, not another unbounded symbolic prefix example.

## Primary Art And Scientific Boundary

[NIST DLMF Section 27.15](https://dlmf.nist.gov/27.15) states the coprime CRT and describes computation with smaller residues. The generalized compatibility argument used here is proved above by applying that theorem to prime powers; the algorithm itself uses gcd/lcm and does not factor the periods.

[Gibson, A Density Chinese Remainder Theorem (2014)](https://math.colgate.edu/~integers/o22/o22.pdf), Theorem 1 and Section 3 equations (13)-(17), explicitly gives generalized compatibility and the two-mask gcd-histogram dot product. Section 4 decomposes an interval into complete gcd-length blocks plus a remainder. The lead inspected these passages after the independent review. The three-mask/event compiler is not stated there; its elementary counting and interval ingredients are established.

[Kim and Glass, Perfect periodic scheduling for three basic cycles](https://link.springer.com/article/10.1007/s10951-013-0331-3), Section 4.1, removes exactly our private factors. With g the triple gcd, their pair factors d_ij=g_ij/g give reduced periods g*d_ij*d_ik=h_i. Their objective is scheduling feasibility for client populations, not cardinality for three prescribed masks or graph-local output. Both the independent reviewer and lead inspected the relevant definitions; this is direct shared-factor precedent, not an analogy.

[Bakibayev et al., Aggregation and Ordering in Factorised Databases (2013)](https://www.vldb.org/pvldb/vol6/p1990-zavodny.pdf), Section 3.2.1, counts disjoint unions by sums and Cartesian products by products. Our conditioned compatible-tuples representation instantiates that counting algebra. Its representation-size bound does not make our periodic sweep run-linear. Passage inspection is attributed to the independent reviewer.

[Revesz and Cai, Efficient Querying and Animation of Periodic Spatio-Temporal Databases](https://cse.unl.edu/~revesz/papers/AMAI02.pdf), Lemma 19, composes periodic intersections over a local lcm. Its polynomial-time argument assumes a fixed finite allowed period set closed under lcm and additional restrictions; it is not an arbitrary-binary-period bound. The independent reviewer retrieved the full text. Terenziani and Kabanza full-text gaps remain explicitly recorded in that review.

The prior voltage, aggregate-join and temporal-interval sources in the [hybrid review](Triangles-Hybrid-Independent-Review.md) remain relevant. A direct fetch of Hu et al.'s temporal PDF failed during implementation; no new runtime theorem is attributed to that failed fetch. The completed shared-residue review found close published ingredients but did not establish historical priority for an identical full graph compiler. This is a bounded search, not an exhaustive novelty clearance.

## Stronger Fixed-Dimensional Counting Comparator

For each choice of one disjoint run [a_i,b_i) per mask, form the integer variables (x,k1,k2,k3) and eight inequalities:

```text
0 <= x <= Q-1
a_i <= x-q_i*k_i <= b_i-1, for i=1,2,3.
```

This is a bounded four-dimensional rational polytope. Each satisfying root determines exactly one k_i=floor(x/q_i), and conversely each integer point determines one root. Disjoint input runs make the totals across run triples disjoint. There is no projected-count multiplicity correction.

[Barvinok and Pommersheim, An Algorithmic Theory of Lattice Points in Polyhedra (1999)](https://library.slmath.org/books/Book38/files/barvinok.pdf), Theorem 4.4 and Algorithm 5.2, supplies exact counting with polynomial bit complexity in fixed dimension. The reviewer inspected these passages. Applying it here gives O(R1*R2*R3*P(b)) bit work for a fixed polynomial P and maximum endpoint/period bit length b, plus normalization and the L/Q multiplier. This reduction is an ordinary instantiation of a published primitive, not evidence that the paper contains this graph compiler.

No such backend is installed, implemented or benchmarked in this study. Constants, transient storage and practical crossover remain unmeasured; the run-triple product can be poor when masks have many intervals. Nevertheless it defeats any claim that numeric-period traversal is intrinsically necessary, or that our event sweep is the first exact route around it.

## Research Decision After Review

Keep three comparator families distinct: event sweeps; elementary predicate simplification and sparse CRT; fixed-dimensional counting. The [elementary same-source control](Triangles-Elementary-Counting-Control.md) is now implemented and solves the review's equal-period and sparse-singleton adversaries without repeated events. It leaves the general coupled-mask event fallback in place. A nontrivial graph-specific composition, selector theorem, or measured useful source regime must survive those controls. The separate code review supports the current implementation's logical behavior subject to its recorded fix and limitations; it supplies no novelty or physical-resource evidence.

Current defensible result: an explicit exact native-periodic triangle compiler with a CRT marginalization theorem, a qualified no-increase cyclic-boundary bound, a complete-output algorithm, finite independent oracles and paid cost formulas. Global novelty, superiority to the strongest same-source controls, practical source prevalence and publication readiness remain unproven. The other six family contributions remain part of the active goal.

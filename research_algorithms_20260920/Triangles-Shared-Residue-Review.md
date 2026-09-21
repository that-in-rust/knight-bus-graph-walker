# Independent Shared-Residue Arithmetic And Prior-Art Review

Date: 2026-09-20. Bounded independent sidecar, not a review of the lead's implementation. Read `Novelty-Baseline-Evidence-Policy.md` and `Triangles-Periodic-Refinement.md`. Only this review file was written; probes were fresh in-memory calculations with no imports from project experiments. No commit, shared-document edit, graph resurvey, or production implementation.

## Findings First

1. **The proposed scalar root-count identity is exact.** The proof below resolves the conjecture under the stated integer Boolean-mask contract. It needs neither prime factorization nor independence assumptions about the original masks.
2. **O(input runs) retained histogram state is valid; O(input runs) total work is not.** After gcd/lcm setup, a coalesced three-stream sweep costs `O(1 + R log(R+1) + E)` arithmetic operations, where `E = sum_i B_i*(H/h_i)` and `B_i` counts nonzero cyclic histogram jumps. Even three one-run masks can force exponentially many sweep steps in the bit length of the periods. Avoiding an H-sized allocation is not avoiding H-scale work.
3. **There is unusually close primary precedent.** Gibson explicitly counts two modular sets by a gcd-histogram dot product and decomposes intervals into complete gcd blocks plus a remainder. Kim and Glass explicitly strip three periods down to exactly these `h_i` values in periodic scheduling. Factorised database counting already supplies sum-of-products aggregation. Precise inspected passages and limits appear below.
4. **A stronger theoretical counting comparator exists.** Each choice of one run from each mask reduces to counting lattice points in a bounded four-dimensional polytope. An inspected Barvinok-Pommersheim algorithm gives polynomial bit complexity for fixed dimension. The reduction here is an ordinary instantiation, not a claim that their paper presents this periodic-mask compiler, and not a measured speed or memory win.
5. **Prospective compiler novelty remains open and narrower than the primitive.** The exact native-mask-to-local-motif composition may still have a useful contribution, but the CRT criterion, removal of private period factors, residue histograms, interval remainders, and sum/product counting cannot individually carry it. No inspected source establishes the entire proposed graph compiler, and an independently constructed control is not historical priority evidence.

## Exact Identity

### Contract

Let `q1,q2,q3` be positive integers dividing `L`. Each `Ui` is a Boolean subset of integer residues `[0,qi)`, represented by disjoint half-open integer runs. All constraints refer to the same root coordinate; any shifts must already have been incorporated into the masks. Define

```text
gij = gcd(qi,qj)
hi  = lcm(gij,gik), where {i,j,k} = {1,2,3}
H   = lcm(g12,g13,g23)
Q   = lcm(q1,q2,q3)
Ai(s) = #{u in Ui : u = s (mod hi)}

C(L) = #{x in [0,L) : x mod qi is in Ui for every i}
     = (L/Q) * sum_{t=0}^{H-1} product_{i=1}^3 Ai(t mod hi).
```

Intervals describe integer points, not continuous lengths or multiplicities. Overlapping input runs must be normalized as a Boolean union or rejected before adding their histogram contributions. Empty masks yield zero. Nonminimal declared periods are allowed, although they can make the sweep unnecessarily expensive.

### Proof

First, `hi` divides `qi`, because both incident gcds divide `qi`. Also

```text
gcd(hi,hj) = gij, and lcm(h1,h2,h3) = H.
```

For the gcd equality, `gij` divides both `hi,hj`, while any common divisor of `hi,hj` divides both `qi,qj`, hence divides `gij`.

The generalized CRT says that a residue tuple `(u1,u2,u3)` is realizable precisely when `ui = uj (mod gij)` for every pair. Any two realizing roots differ by every `qi`, so a compatible tuple specifies exactly one root modulo `Q`. Conversely every root modulo `Q` specifies one such tuple. The compatibility criterion is stated in Gibson, Theorem 1; the histogram precedent is detailed in the source ledger below.

For a compatible tuple, put `si = ui mod hi`. The displayed gcd equality makes `(s1,s2,s3)` CRT-compatible, so it specifies exactly one `t mod H`. Conversely, for fixed `t mod H`, any choices `ui in Ui` with `ui = t (mod hi)` are pairwise compatible modulo `gij`. There are exactly `product_i Ai(t mod hi)` such choices. The groups indexed by `t` are disjoint, exhaust all compatible tuples, and therefore count roots modulo `Q` exactly once. Finally `Q` divides `L`, giving the multiplier `L/Q`.

This is a proof by explicit bijections, not an appeal to probabilistic independence. The same argument extends to any fixed number of masks using `hi = lcm_{j != i} gcd(qi,qj)` and `H = lcm_i hi`; its algebra is not specific to triangles.

### Useful Checks And Non-Checks

- Prime by prime, the exponent in `H` is the middle of the three exponents in `q1,q2,q3`. Thus `H` divides `Q`, but can equal `Q`. This characterization is explanatory; computing gcd/lcm needs no factorization.
- `Q = H*product_i(qi/hi)` and `q1*q2*q3 = Q*H*gcd(q1,q2,q3)`. Full masks therefore return exactly `L`.
- Pairwise coprime periods give `hi=H=1`, so `C(L)=(L/Q)*|U1|*|U2|*|U3|`.
- Do not replace every `hi` with the triple gcd. For `q=(2,4,4)` and singleton masks `{0},{0},{2}`, that replacement returns 1 at `L=4`, whereas the exact answer is 0.
- Do not assert `gcd(H,Q/H)=1`. For `q=(8,4,2)`, `H=4` and `Q/H=2`. The proof does not split these two moduli by a coprime CRT.
- The summands are counts of residue tuples, not a root indicator of period `H`. With `q=(2,3,5)` and all three masks `{0}`, `H=1` but the satisfying roots have period 30. This scalar primitive alone does not establish the lead's local-output representation.

## Histogram And Sweep Accounting

For one canonical run `[a,b)` and divisor `h` of its native period, let `ell=b-a`, `c=floor(ell/h)`, and `r=ell mod h`. Its histogram contribution at residue `s` is exactly

```text
c + 1[((s-a) mod h) < r].
```

Each complete block of `h` consecutive integers contributes one to every residue. The remaining `r` points form one circular arc starting at `a mod h`. Add its constant `c` and, when `r>0`, a +1 event at its start and a -1 event at its end, both modulo `h`. A wrapping arc contributes at residue zero as well. Equal-position deltas must be combined, and zero net deltas removed. When `r=0`, there is no arc and no jump, including for a full native mask.

For `R_i` input runs, this requires `O(R_i)` event records before coalescing, plus a scalar base value. Modulo reduction can scramble the original ordering, so sorted native runs do not imply sorted event positions. Comparison sorting costs `O(R_i log(R_i+1))`; merely constructing unsorted events is linear. The state claim is about integer records, not constant-size bytes.

Let `R=sum_i R_i`, and let `B_i` be the number of nonzero cyclic changes in the coalesced histogram in `[0,h_i)`, including the change across zero when nonzero. Then

```text
B_i <= min(2*R_i, h_i)
E = sum_i B_i * (H/h_i) <= 3*H.

Setup:        a fixed number of exact gcd/lcm computations
Preprocessing: O(R log(R+1)) further arithmetic/comparison operations
Sweep:        O(1+E) arithmetic operations for three streams
Retained:     O(R+1) integer records, including the input/event lists
```

A stream retains one local sorted event list, a cursor, and a repetition index, not all `H/h_i` copies. Merge the next positions of the three streams, multiply the three current values by the integer segment length, and apply all deltas at a tied boundary before continuing. Initializing at zero avoids requiring a synthetic jump for constant histograms. Counting zero as an initial event rather than initialization changes at most three event visits in these formulas. A heap over three streams is unnecessary. No H-sized array is required.

These are arithmetic-operation bounds. Endpoints, lcms, jumps, products, and the accumulator require exact integers; source widths and multiplication/division costs remain paid. The numerator counts at most `Q` roots, and the final scalar at most `L`, but that does not justify unit-cost arbitrary-precision arithmetic. A fixed-width implementation must also avoid overflowing intermediate lcm products. This review makes no RSS or physical-memory certification.

### A Concrete Work Counterexample

For even `N>=2`, set

```text
q = (2,2N,2N), L=Q=H=2N, h=(2,2N,2N)
U1=[0,1), U2=[0,N), U3=[0,N).
```

There are only three runs and `B=(2,2,2)`, but `E=2N+4`. The merged streams change at every integer position, giving `2N` cyclic boundary positions. The answer is just `N/2`. For `N=2^k`, a literal boundary sweep is exponential in the O(k)-bit input, despite constant retained event-record count. Merging the duplicate large masks and applying a two-mask floor-count formula avoids this work; this is a counterexample to the proposed sweep's worst-case time, not a lower bound on the problem.

Even distinct minimal native periods can lose the proposed advantage: for pairwise coprime integers `a,b,c>1`, use `q=(ab,ac,bc)` and three singleton `{0}` masks. Then `hi=qi`, `H=Q=abc`, and the literal streams have `E=2(a+b+c)` events. Ordinary singleton CRT returns one root modulo `Q` from a constant number of gcd compatibility checks. This analytical family prevents attributing the first counterexample solely to duplicate period declarations.

## Primary Sources Actually Inspected

The following passages were read directly, not inferred from titles, abstracts, or third-party summaries. Source versions matter: the Gibson journal PDF numbers its theorems differently from the arXiv PDF.

### 1. Closest Modular Counting Precedent

[D. Jason Gibson, A Density Chinese Remainder Theorem, Integers 14 (2014), A22](https://math.colgate.edu/~integers/o22/o22.pdf): read Theorem 1 (p. 1); Theorem 3 and Corollary 1 (pp. 2-3); Section 3, equations (13)-(17) (pp. 4-5); and Section 4 (pp. 7-8). Section 3 gives the exact two-mask gcd-histogram dot product. Section 4 splits each interval into full gcd-length blocks and a remainder; its main theorem is a lower bound, not an exact arbitrary-position remainder-intersection algorithm. The exact three-mask formula and streamed boundary schedule are not stated in these passages. **Classification:** published method for the two-mask primitive; explicit CRT/aggregation extension here, not evidence of a new CRT principle. The arXiv copy was also consulted; use journal numbering for citations.

### 2. Three-Period Shared-Factor Reduction

[Eun-Seok Kim and Celia A. Glass, Perfect periodic scheduling for three basic cycles, Journal of Scheduling 17 (2014), 47-65, online 2013](https://link.springer.com/article/10.1007/s10951-013-0331-3): read Section 2's CRT/Lemma 1, Section 3's residue partition and Lemmas 3-4, and Section 4's factor definitions through Section 4.1's Theorem 2 and Lemma 5 with proofs. Their notation uses `g=gcd(q1,q2,q3)`, `dij=gij/g` (renamed here to avoid collision), and private factor `gi0=qi/(g*dij*dik)`. Therefore their reduced period `qi/gi0` equals our `hi` exactly. This is explicit published removal of private period factors. Their theorem concerns choosing a collision-free schedule for client populations, not counting intersections of three prescribed interval masks. **Classification:** published shared-period reduction; no verified priority for this exact histogram compiler.

### 3. Factorised Counting

[Nurzhan Bakibayev, Tomas Kocisky, Dan Olteanu, and Jakub Zavodny, Aggregation and Ordering in Factorised Databases, PVLDB 6(14), 2013](https://www.vldb.org/pvldb/vol6/p1990-zavodny.pdf): read Section 3's operator contract, Section 3.1, and Section 3.2.1 (printed pp. 1993-1994). The counting procedure adds cardinalities across disjoint unions and multiplies them across products; partial aggregation can precede later operations. Our compatible tuple set factors as the disjoint union, over `t mod H`, of the three sets selected by `ui mod hi=t mod hi`. Applying that count rule yields the proposed numerator. Materializing those groups or the full factorisation is not free: the periodic event representation is a separate implementation choice. **Classification:** ordinary instantiation of published counting algebra; its linear-in-factorisation bound is not automatically linear in native runs.

### 4. Periodic Temporal Intersection

[Peter Revesz and Mengchu Cai, Efficient Querying and Animation of Periodic Spatio-Temporal Databases, author preprint associated with the 2002 publication](https://cse.unl.edu/~revesz/papers/AMAI02.pdf): inspected the 26-page author PDF's pp. 13 and 16-18, especially the assumptions before Lemma 16 and Lemma 19's intersection construction. Lemma 19 composes two periods by their local lcm and processes copies within one combined period. Its PTIME argument bounds the ratios using a fixed finite allowed period set `K`, closed under lcm, together with slope restrictions. Those assumptions do not cover arbitrary binary-encoded periods in this task. **Classification:** published periodic intersection with local period composition, not the shared-residue scalar count or its claimed event-space contract. It does refute treating local periodic intersection or avoiding all repetitions over an infinite horizon as new principles.

### 5. Fixed-Dimension Exact Counting

[Alexander Barvinok and James E. Pommersheim, An Algorithmic Theory of Lattice Points in Polyhedra, MSRI Publications 38 (1999)](https://library.slmath.org/books/Book38/files/barvinok.pdf): read Theorem 4.4 and its proof, and Section 5 through Algorithm 5.2 (printed pp. 109-112). These give short rational generating functions and polynomial-time exact lattice-point counting in fixed dimension. The original [Barvinok 1994 publication page](https://pubsonline.informs.org/doi/10.1287/moor.19.4.769) was checked for provenance; the inspected algorithm text is the author/coauthor 1999 chapter, not a claimed reading of the 1994 full paper. **Classification:** published general counting algorithm; the reduction below is our explicit instantiation and was not implemented or benchmarked here.

### Access Gaps And Search Boundary

- Terenziani's 2003 `Symbolic user-defined periodicity in temporal relational databases` remains a relevant lead, not an inspected procedure: its [institutional record](https://iris.uniupo.it/handle/11579/14074) has no file, and the located CiteSeer PDF failed to open.
- The related 2013 `An intensional approach for periodic data in relational databases` has an [institutional record with no file](https://iris.uniupo.it/handle/11579/70715). A differently titled five-author `An implicit approach...` draft was discoverable, but its CiteSeer download timed out; it was not treated as the verified four-author journal procedure. An attempted institutional technical-report URL returned 404.
- [Kabanza, Stevenne, and Wolper, Handling infinite temporal data (1995)](https://orbi.uliege.be/handle/2268/163842) was inspected only at its institutional abstract/metadata page, which explicitly has no full text. No theorem-level claim here rests on it.
- Searches covered modular interval intersection/counting, generalized CRT, three-cycle scheduling, periodic temporal relational operations, factorised counting, and fixed-dimensional exact lattice counting. This is a bounded nearest-method check, not an exhaustive novelty search. Source search-result crawl dates were not used as publication dates.
- Revesz-Cai succeeded via `curl` piped into the bundled `pypdf` reader, entirely in memory, after web-tool failures. No downloaded PDFs or probe files were retained. Other failed retrievals are not silently counted as full-text reads.

## A Published-Primitive Comparator With Bit-Polynomial Cost

This is a reduction derived in this sidecar on 2026-09-20, independently of the lead's proposed histogram/sweep implementation.

Choose one disjoint run `[ai,bi)` from each mask and count integer points `(x,k1,k2,k3)` satisfying

```text
0 <= x <= Q-1
ai <= x-qi*ki <= bi-1, for i=1,2,3.
```

These are eight integer-coefficient linear inequalities in four variables. They define a bounded rational polytope: `x` is bounded, and each `ki` is then bounded by its two inequalities. Because `0<=ai<bi<=qi`, every satisfying root has exactly one quotient `ki=floor(x/qi)` for each i, and conversely every lattice point supplies a satisfying root. No many-to-one projection or multiplicity correction is needed.

Invoke the inspected fixed-dimensional counting algorithm for each run triple and add the counts. The masks' disjointness ensures that each root belongs to at most one run triple. If `b` is the maximum endpoint/period bit length, `log Q <= sum_i log qi = O(b)` for three masks. The total cost is

```text
normalization + O(R1*R2*R3 * P(b)),
```

for the fixed-dimension algorithm's polynomial `P`, followed by multiplication by `L/Q` at the appropriate bit width. This uses no global period across unrelated motifs, imports no shared-residue sweep step, and does not expand `Q` roots. The reduction is valid for unequal periods and long intervals, unlike a singleton-only CRT shortcut.

This defeats a claim that numeric-period traversal is necessary for exact three-mask cardinality, or that this sweep is the first route to avoiding such traversal. It does **not** establish a practical replacement: no library/backend was installed, no Barvinok execution occurred, the exponent/constants and transient storage are unpaid experimentally, and `R1*R2*R3` can dominate an event sweep with many runs. A meaningful compiler selector may exploit that distinction.

## Executed Independent Probes

Executed using `python3.11 -B` with standard-library integers, `math.gcd/lcm`, `itertools.product`, and `random.Random(20260920)`. The direct oracle scanned every integer root of the small chosen `L` and tested native mask membership. The formula side independently built dense histograms by counting native residues in each congruence class. These probes verify mathematics, not streamed storage or lead code.

| Probe | Actual cases | Result |
| --- | ---: | --- |
| All ordered `q1,q2,q3 in {1,2,3,4}` and every Boolean subset for each period; `L=Q` | 27,000 | Zero formula/oracle mismatches |
| Seeded ordered periods in `[1,24]`, random bitsets, multiplier `L/Q` in `[1,3]` | 1,000 | Zero formula/oracle mismatches |
| Every nonempty `[a,b)` for every `q in [1,40]`, every divisor `h` of q; all histogram residues checked | 53,205 interval/divisor cases | Zero floor-plus-circular-remainder mismatches |

The exhaustive count is `(2+4+8+16)^3=27,000`, not 27,000 distinct period triples. Random generation drew the three periods, then their bitsets with `getrandbits(qi)`, then the multiplier, from one sequential seeded RNG. The identity checks also asserted `gcd(hi,hj)=gcd(qi,qj)` and `q1*q2*q3=Q*H*gcd(q1,q2,q3)` on every mask case. Empty and full masks, repeated moduli, divisibility chains, and coprime periods are included in the exhaustive domain.

Explicit scalar fixtures executed at `L=Q`:

| Periods | Native masks, or integer bitsets with bit u denoting residue u | h | H | Q | Exact roots |
| --- | --- | --- | ---: | ---: | ---: |
| `(6,10,15)` | `[0,3)`, `[0,5)`, `[0,7)` | `(6,10,15)` | 30 | 30 | 4 |
| `(4,6,9)` | bitsets `(11,45,307)` | `(2,6,3)` | 6 | 36 | 9 |
| `(2,3,5)` | `{0}`, `{0}`, `{0}` | `(1,1,1)` | 1 | 30 | 1 |
| `(2,4,4)` | `{0}`, `{0}`, `{2}` | `(2,4,4)` | 4 | 4 | 0 |

The literal-sweep counterexample was independently counted using dense small histograms to measure all cyclic changes. Event counts below include cyclic position zero once; an implementation initialized at zero can avoid up to three corresponding visits.

| N | H=Q | Native runs | B | Repeated stream events E | Distinct merged cyclic positions | Exact roots |
| ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 8 | 16 | 3 | `(2,2,2)` | 20 | 16 | 4 |
| 64 | 128 | 3 | `(2,2,2)` | 132 | 128 | 32 |
| 1,024 | 2,048 | 3 | `(2,2,2)` | 2,052 | 2,048 | 512 |
| 8,192 | 16,384 | 3 | `(2,2,2)` | 16,388 | 16,384 | 4,096 |

These are actual logical counts, not timings, RSS, a production benchmark, or evidence of a Barvinok speedup. The distinct-period singleton family above is analytical, not included in these measured rows. No test-first or implementation-verification claim is made on the lead's behalf.

## Contribution Boundary And Handoff

The conventional baseline is generalized CRT with exact aggregation. Three alternative evaluation routes are now explicit: shared-residue events, direct sparse/singleton CRT and simplified two-mask counting, and fixed-dimensional lattice counting. None is a universal practical winner under this review's evidence.

A defensible prospective delta would need to reside in a **specified compiler/composition/selection result**: how native shifted runs feed motif-local constraints; what reusable intermediate contract is produced; when histogram events are selected over simpler arithmetic or other exact methods; and what fully paid work/storage bound or measured application regime is gained. The lead owns graph correctness, local outputs, compiler implementation and its tests, output-memory accounting, and manuscript claims. They were deliberately not reimplemented here.

For the next claim ledger, keep these categories separate:

- **Published:** two-mask residue histograms, the exact three-period shared-factor reduction, periodic local-lcm intersections under their published contract, factorised cardinality, fixed-dimensional lattice counting.
- **Ordinary instantiation derived here:** the bijective three-mask sum/product proof, interval-to-four-dimensional-polytope reduction, and the associated arithmetic-operation accounting.
- **Suggested constructed control:** a same-source selector using singleton/equal-period/two-mask simplifications and an event budget. This is an unimplemented design suggestion, not a complete measured comparator and not historical evidence for an identical compiler.
- **Still open:** whether a nontrivial graph-specific compiler theorem or useful empirical separation survives those comparisons; whether the unavailable temporal papers contain a closer exact procedure; and whether the event representation plus local-output contract offers a contribution beyond straightforward composition.

**Decision:** accept the scalar identity as proved; accept compact retained event state with explicit bit-width caveats; reject any unqualified run-linear or bit-polynomial runtime claim for the literal sweep. Do not claim a new CRT/counting primitive. Do not dismiss the entire prospective compiler merely because a control could copy it. This sidecar does not close the seven-algorithm research goal.

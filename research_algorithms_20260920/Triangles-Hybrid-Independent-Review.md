# Independent Review: Hybrid Triangle Ownership

Date: 2026-09-20. Bounded sidecar review of FFF/P**/FP*/FFP ownership, exact convolution, and their cost claims. Only this file was written; no source edits, journal edits, or commit. Periodic-mask quotient algebra was excluded.

## Findings First

1. **No new valid-profile counting or integer-convolution bug found.** The mixed theorem is correct for validated Boolean cross-base masks. The implementation preserves the original root when changing join orientation and does not pass FFF triples through the partial enumerator. This conclusion rests on the proof below and fresh checks, not the old 4,096-graph suite.
2. **The closest-art relationship is stronger and more precise than merely "related to voltage algebra."** Published voltage-walk coefficient counting and circular convolution directly specialize to the full-class kernel. Published aggregate-join distributivity directly yields the four ownership terms. These are ordinary instantiations of documented primitives, not evidence based on a hypothetical competitor copying this implementation.
3. **The paid logical bound and event bound are supported at their stated scope.** `full_closed_triples` is a numerical answer, not an enumeration counter. However, `sum_t F_t` is an explicit unexpanded backend cost, not a proved near-linear runtime for this Python probe. The byte footprint is proportional to `L*width`, not uniformly to L bytes.
4. **No universal speedup, bounded-memory execution, or paper-level novelty is established.** Unconditional convolution on nonempty full subsets has a simple sparse adversary, already acknowledged by the manuscript. The remaining potential contribution is the encoded-source composition and its resource-aware execution, not a new partition identity or convolution theorem.

No source correction is requested by this review. The publication-facing correction is to make the closest-art reductions explicit and keep the claim boundary below.

## Scope And Snapshot

Read the [hybrid manuscript](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Triangles-Hybrid-Orbit-Ownership.md), [probe](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_masked_matching_triangles.py), [existing independent review](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Triangles-Masked-Matching-Review.md), and [novelty policy](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Novelty-Baseline-Evidence-Policy.md). The existing test source was inspected to avoid presenting its coverage as new evidence. The old validation findings are historical; they are not re-reported as current defects.

The reviewer's graph query returned no entries for the probe, so direct source reads were used without rebuilding an index. The lead's separate graph query did return the hybrid functions; the review query result is not a claim about all current project indexes. Tests ran in memory through inline `python3 -B` commands, on Python 3.9.6. Neither existing exhaustive suite nor the lead's random suites were run. No timing, RSS, external-memory, or fast-convolution-backend claim follows from these checks.

Reviewed SHA-256 values, relative names within the research directory:

```text
c544933f15d78718fff9f74cc86e1d89aea3711b5378e7e58bdd14e95d2e96a5  Triangles-Hybrid-Orbit-Ownership.md
b2640cb46a9e04a8ac7700c645c0e16de0108fb8e414cd7c2a333c0bcbef65a0  experiments/probe_masked_matching_triangles.py
db38de5c8e93bf2ee398242a941fd9a4bca7af046a013001cec2cbd9466d7016  Novelty-Baseline-Evidence-Policy.md
```

The prior review initially hashed `c4c88102dd3f67020be2de61c6e62a3e083e6ad4ac256763b33ddad79f271dee`. During this sidecar, the lead added an integration-status note; the subsequently inspected version hashed `064be5b5cae14631bc4048e05cf33410c16404fc797b9e9feca30053dc5df8ff`. That external edit was preserved. The hybrid manuscript and probe hashes stayed unchanged through the pre-write check.

## Supported Proof

### Ownership And Coordinates

Fix distinct bases `a<c<z`. Let `G_i=F_i disjoint-union P_i` be displacement classes on positions `(a,c),(c,z),(a,z)`, after coalescing masks and removing empty classes. These are partitions of classes, not a division of the coordinates of a partial mask into "full" and "partial" points.

For a concrete triangle with coordinates `(g,h,j)`, its unique displacement tuple is

```text
x = h-g mod L, y = j-h mod L, d = j-g mod L;
x+y = d mod L.
```

Every tuple belongs to exactly one domain:

```text
F0 F1 F2
P0 G1 G2
F0 P1 G2
F0 F1 P2
```

The latter three expand respectively to four, two, and one of the seven non-FFF status words. Thus simultaneous partial classes cause neither a missed cross-term nor double counting. "First partial" is only an ownership convention; it assumes no chronological ordering of graph updates.

The [enumerator](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_masked_matching_triangles.py:135) chooses exactly one pair of positions. Positions `(0,1)` derive `d=x+y`; `(0,2)` derive `y=d-x`; `(1,2)` derive `x=d-y`. The common `y-x` expression in the last two code branches is correct because those local loop variables denote the selected positions in increasing order. Each chosen pair determines exactly one candidate tuple.

After membership succeeds, the code reconstructs `(x,y,d)` and uses

```text
I = S_ac(x) intersect S_az(d) intersect (S_cz(y)-x).
```

Its event destinations remain `g`, `g+x`, and `g+d`. Changing enumeration order never changes the root. This is sufficient even when a valid closing tuple has an empty intersection.

The [base traversal](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_masked_matching_triangles.py:150) visits a triangle only through its two smallest bases: `c>b` in code. The other two edges cannot visit that same ordered triple. Cross-base-only edges ensure every concrete triangle has three distinct bases, so this covers the whole supported domain.

### Full Contributions And Exact Arithmetic

For full displacement sets A, B, D, the number of closing pairs is `k=sum_(d in D) (1_A *_L 1_B)[d]`. Each pair yields L distinct concrete triangles. Fixing a coordinate at any of the three fibres uniquely determines the root, so every vertex in each fibre receives k, not merely an average of k. Contributions from different base triangles add normally.

For the [packed backend](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/experiments/probe_masked_matching_triangles.py:122), put `w=max(1,ceil(bit_length(L)/8))` and `B=256^w`. Each coefficient of the ordinary product of two Boolean indicator polynomials is at most L. Since `B>L`, evaluating the polynomials at B gives their exact coefficients as carry-free base-B digits. This holds also when L is an exact power of 256: the width increases at that boundary.

The product has at most `2L-1` coefficients. Encoding it into `2L*w` bytes leaves the coefficient at index `2L-1` zero. Reading digits d and d+L and adding them after decoding therefore implements cyclic folding exactly, including `d=L-1`. There is no cross-digit carry from the fold. `k<=|A||B|<=L^2`; Python integer addition and weighted events retain that multiplicity. No floating-point assumption, prime-modulus assumption, or signed-convolution reduction is used.

Degree emission is independent of ownership, and includes every actual edge mask. Summing exact weighted events over every declared coordinate includes isolated vertices. Together these facts prove complete row and global-count equivalence, not just equality of total triangles.

## Cost Audit

The manuscript's lines 76-95 have a consistent **logical work** interpretation:

- `W_B`: the smaller-neighborhood scans really pay `sum_(base edges uv) min(deg_B(u),deg_B(v))` neighbor probes, in addition to index construction. This is not automatically linear in source size.
- `D_B`: each base triangle reconstructs its full/partial dictionaries by scanning the three shift dictionaries. Constantly many passes give the stated charge. No free global classification cache is assumed.
- `C_P`: for each ownership pattern, exactly the minimum of the three pair products is probed, including zero for empty domains. A closing partial tuple is visited once. No FFF tuple enters this loop.
- `J_P`: charges mask alignment/intersection for surviving partial tuples; it is not a measure of final answer size. `Q_P<=C_P` and the nonempty integer-run model bounds event work by this charged interval work.
- `F_t`: must include indicator construction, packed multiplication, product serialization, coefficient lookups/folding, and summation, even when k=0. The implementation decodes only the two relevant coefficients per closing displacement, rather than building a separate folded vector.

One backend invocation makes two length-L indicator scans, one multiplication of integers having at most `8Lw` bits, one `2Lw`-byte product, and `2|D|` width-w coefficient reads. If `Mul(n)` denotes the chosen runtime's n-bit multiplication cost, that operation must be charged as `Mul(O(Lw))`, not one constant-time arithmetic operation. The join/build temporary objects, live operand integers, and multiplication scratch also occupy memory. Consequently "length O(L)" is a coefficient/digit count; the explicit product buffer is `2Lw` bytes. The `N<2^64` profile bounds w by eight, but does not make a large allocation admissible.

Initial degree events remain bounded by `4R+2M`. Partial event records satisfy `min(10H_P,6H_P+4Q_P_plus)` because a rotation introduces at most one extra cut across a disjoint mask. Each positive full aggregate contributes exactly six records. Thus

```text
E <= 4R+2M + min(10H_P,6H_P+4Q_P_plus) + 6B_F_plus
```

is valid. Event-map reduction can make the retained key count smaller; it does not retrospectively erase emission work. The old one-bit payload is insufficient for k, while the stated 256-bit signed allowance is conservative for `N<2^64`. This says nothing about fixed-width address/product arithmetic or successful allocation.

The bound is not a complete Python worst-case bit-time or external-memory theorem. It presumes charged lookup/index operations and the source normalization contract; arbitrary-input sorting, serialization, physical sorting/I/O, and output retention still need their own charges. Complete numeric output costs `Theta(bL)` rows regardless of convolution success. These are qualifications of the disclosed model, not newly reproduced bugs.

**Executed sparse adversary:** `L=65536`, three full singleton sets `A=B={0}`, `D={1}`. The probe performs one convolution although k=0; it returns 196,608 rows `(2,0)`, with zero partial probes and zero full triangle events. A pair-membership test could reject the closure in constant shift work after reading the three classes. This confirms the disclosed selector weakness. It does not prove an asymptotic end-to-end separation when the common full-row output already costs `Theta(L)`.

## Fresh Executed Evidence

All assertions passed in two inline runs. The graph oracle directly expanded edges, built undirected neighbor sets, and enumerated concrete triangles `u<v<w` by neighbor intersection. It did not call candidate interval helpers or convolution. Each expected local count was incremented at all three concrete vertices. Additional instrumentation temporarily wrapped functions in memory, without editing source.

| Check | Fresh inputs and result |
| --- | --- |
| Ownership words | Eight fixtures, one per F/P status word, L=11, shifts `(9,5,3)`, legal root set `{0,3,10}`. Partial second-edge coordinates were translated by 9. Full masks were supplied as reversed adjacent unit intervals to test coalescing before classification. All rows matched; fourth base isolated. |
| Mixed graphs | 60 fixtures, seed `20260920517`, seven bases with the last isolated, periods cycling through `3,5,9,11,17`. All degrees, local counts, and totals matched the independent adjacency oracle. |
| Shared fibres | One irregular all-full five-base graph plus an isolated sixth base, L=19, displacement d on pair `(a,c)` iff `(a+2*c+3*d)%7 in {0,1,4}`. Overlapping base-triangle aggregates matched every row. |
| Degenerate period | One three-base full triangle plus isolated fourth base, L=1. Correct counts; no possible nonempty partial canonical mask. |
| All join orientations | At L=13, domain sizes `(2,3,9)`, `(2,9,3)`, `(9,2,3)` uniquely force each of the three pair choices. Each yielded four closing tuples using six probes, equal to a direct three-domain product filtered by closure. |
| Packing thresholds | L in `{1,2,255,256,257,65535,65536,65537}`. Dense A=B=Z/LZ with `D={0,L-1,floor(L/2)}` returned `L*|D|`. Singleton `(L-1,L-1)` checks exercised wrap and nonclosure. |
| Irregular coefficients | At L in `{7,31,255,256,257}`, all 806 individual cyclic coefficients matched an independently accumulated pair histogram. Supports were `A={g:(g*g+3*g+1)%11<7}` and `B={g:(5*g*g+g+3)%13<8}`. Summing over all closing displacements also returned `|A||B|`. |
| Counter/event accounting | 24 further fixtures, seed `20260920603`, five bases, periods `7+case%5`. Independently enumerated class domains, closures, and legal coordinate sets verified W_B, C_P, Q_P, positive partial/full counts, and the sharper event bound. |

The first four rows comprise **70 graph-oracle fixtures**, not 70 additional exhaustive suites. Across them, the independent concrete-triangle status histogram was:

```text
FFF=18066  FFP=8907  FPF=9371  FPP=6442
PFF=9471   PFP=6281  PPF=6306  PPP=4288
```

For every such fixture, `L*full_closed_triples` equaled the oracle's FFF count. Captured partial-emitter tuple identities were unique, and none had three full masks. Across the 24 accounting fixtures, totals were `C_P=5485`, `Q_P=3511`, `Q_P_plus=2274`, and `Q_F=167`. These counters are not elapsed-time measurements.

Reproduction details for the random inputs: iterate base pairs in `itertools.combinations` order, then displacements in increasing order. Draw `state=rng.randrange(5)`; state 0 uses every coordinate, states 1 and 2 use coordinates satisfying `rng.randrange(3)==0`, and states 3 and 4 omit the class. For the 60-case run, build unit intervals in increasing coordinate order, shuffle with the same RNG, and retain even empty interval lists; the active bases are 0 through 5. For the 24-case run, omit empty supports, retain sorted unit intervals, and do not shuffle. These are separate freshly seeded generators.

The orientation supports were, in order, `({0,12},{1,6,10},set(range(9)))`, `({0,12},set(range(9)),{1,6,10})`, and `(set(range(9)),{0,12},{1,6,10})`. Packing widths at the eight tested periods were respectively `1,1,1,2,2,2,3,3`; dense results were `1,4,765,768,771,196605,196608,196611`.

## Closest Primary Art

These comparisons follow the policy's distinction between a published method, an ordinary instantiation, and a newly constructed control. Source text was inspected, not just titles or search summaries.

### Voltage And Circulant Kernel

Dalfo, Fiol, Miller, Ryan, and Siran, **An algebraic approach to lifts of digraphs**, arXiv:1612.08855v2; journal version Discrete Applied Mathematics 269 (2019), 68-76. Sections 3-4 allow multiple voltage arcs; Lemma 4.1 identifies polynomial coefficients with lift-walk counts from every fibre coordinate. The paragraph following it and section 4.1 explicitly use circular convolution/DFT. [Primary text, sections 3-4.1](https://arxiv.org/pdf/1612.08855).

**Our reduction:** encode A, B, and reversed D as the three arc polynomials of the directed base cycle `a->c->z->a`. In the cyclic group algebra, its constant coefficient is

```text
[z^0] A(z) B(z) D(z^-1) = k.
```

Each directed cycle corresponds to one triangle with the fixed base order; thus no factor of six is needed. The lemma supplies the equal per-fibre counts directly. Constructing the arc coefficients costs reading the shift sets; evaluating this single product does not require materializing a full base matrix or computing its spectrum. Backend, base traversal, degrees, and output remain paid. This establishes the all-full formula as an ordinary specialization, but does not supply a partial-mask compiler or bounded-memory schedule.

### Aggregate Ownership Identity

Ahmad, Kennedy, Koch, and Nikolic, **DBToaster: Higher-order Delta Processing for Dynamic, Frequently Fresh Views**, PVLDB 2012. Section 3.2 states product delta rules; section 5, Figure 2 rule (2), and the accompanying text give join polynomial expansion/factorization and distribution through sum aggregates. [Primary text, sections 3.2 and 5](https://dbtoaster.github.io/papers/pvldb2012-dbtoaster.pdf).

**Our reduction:** start from `G0 G1 G2`, substitute `Gi=Fi+Pi`, and distribute successively only within the residual F-prefix. This gives exactly

```text
G0 G1 G2 = P0 G1 G2 + F0 P1 G2 + F0 F1 P2 + F0 F1 F2.
```

Equivalently, insert P2, then P1, then P0 into the all-F database and sum the three batch deltas. Selection by shift closure and summation of legal-root indicators preserve the identity. Disjointness makes this Boolean ownership, without negative corrections. Classification costs D_B here; closure and cyclic interval execution remain additional operators, not free algebraic rewrites. Thus the ownership identity itself is established join algebra, while this exact encoded execution contract is not a runtime theorem obtained from DBToaster.

### Partitioned Triangle Joins

Kara, Ngo, Nikolic, Olteanu, and Zhang, **Counting Triangles under Updates in Worst-Case Optimal Time**, ICDT 2019. Definition 6 and section 3 partition relations and split the triangle aggregate into eight status views. Proposition 8 and Figure 3 choose alternative evaluation directions and use auxiliary aggregates. [Primary text, pages 4:7-4:10](https://drops.dagstuhl.de/storage/00lipics/lipics-vol127-icdt2019/LIPIcs.ICDT.2019.4/LIPIcs.ICDT.2019.4.pdf).

This is closer structural precedent than an unspecified "partitioned join": disjoint relation parts, additive triangle subqueries, and different execution strategies already occur together. However, its partitions are by heavy/light variable degree, not translation-complete versus partial displacement masks. Its scalar dynamic-count contract is not the complete local row table here. Replacing the partition predicate transfers the elementary decomposition, not its heavy/light inequalities or update bound. Expanding masks into ordinary edge relations would incur the expanded input cost; retaining masks instead requires a charged reduction. This source does not establish the hybrid's compressed-input cost or historical priority for its exact compiler.

### Exact Convolution Bound

Bringmann, Fischer, and Nakos, **Deterministic and Las Vegas Algorithms for Sparse Nonnegative Convolution**, arXiv:2107.07625v1 (2021), Theorem 1 and section 2. The deterministic bound is `O(t polylog(n*Delta))`, with t the ordinary product's nonzero support and Delta its maximum coefficient, in the stated word-RAM model. [Primary theorem and model](https://arxiv.org/pdf/2107.07625).

Here `n=L`, `Delta<=L`, and `t<=2L-1`. Reading A/B/D, constructing the backend representation, folding, and querying D are additional charged steps. In particular, t is not k, the number of matching closing shifts, or the number of output runs. This supplies an available mathematical primitive, not the complexity of Python packed multiplication. The manuscript correctly declines to attribute that theorem to its implementation.

The cited Hu et al. temporal PDF could not be fetched directly in this pass. No runtime or priority conclusion here depends on it. The existing warning about tuple-dependent shifts requiring a charged reduction remains appropriate; temporal-join exploration was not expanded beyond this scope.

## Claim Boundary And Next Question

**Supported claim:** given normalized integer Boolean cross-base cyclic matching classes, the implementation exactly separates a translation-complete FFF subquery from every partial-containing triangle, evaluates the former without enumerating its closing shift triples, and combines both into complete degrees/local counts/global count. Its logical cost is the stated charged sum with explicit F_t and full output work.

**Inherited steps:** full-orbit walk counting/convolution and additive partitioned-join algebra have the precise primary antecedents above. Their composition is meaningful engineering, but the correctness identity alone does not demonstrate a new algorithmic theorem. The inspected papers do not by themselves establish that the entire masked-input/compiler/operator contract was previously published. Conversely, this bounded search does not establish its novelty.

**Control categories:** the independent expanded-adjacency oracle constructed in this review is a correctness control, not historical prior art or a fair compressed-input performance benchmark. A new same-mask control combining the published polynomial kernel with the displayed join reduction would be an ordinary-primitives instantiation; importing the candidate's implementation would instead be a candidate-incorporating reimplementation. Neither should be relabeled a previously published end-to-end hybrid.

**Separation actually available:** the all-full route removes an enumerated Q_F term relative to the interval-only plan on native full-mask inputs. Dense full shift classes can require quadratic flat defects from a one-shift cover, so charged compilation can erase an end-to-end subquadratic claim. Equal representation access and identical output obligations apply to every comparator. No performance separation against a matched aggregate-capable published instantiation was measured here.

**Next concrete research question:** can a selector, using only paid source metadata and an admitted working-memory budget, choose between cheapest-pair enumeration and exact convolution for each full subquery, while retaining the partial ownership routes, with a proved total-cost guarantee relative to the best of those allowed plans? Selection, construction, scratch, repeated base-triangle access, and complete output must be included; unknown k/H/Q cannot be a free planning oracle. A guarantee only about estimated costs should be labeled as such.

The first discriminating test should place sparse singleton full sets, irregular dense full sets, and partial-dominated instances in the same native-format/output contract, then measure construction plus query time and peak working storage against the admitted plans. Publish the break-even conditions and any selector counterexample. This is a finite next question about planning and resource accounting, not an invitation to develop periodic-mask quotient algebra or another renamed correctness identity.

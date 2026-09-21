# Community And Triangles: Independent Skeptical Review

Date: 2026-09-20. Scope: CSCJ in `Budgeted-Communities.md`, TDDS in `Factorized-Triangles.md`, and the retained `community-triangles-Probe-Evidence.md`. This review changes neither candidate nor production code. Prior-art searches are bounded, not a global novelty guarantee.

## Premise Check

**Neither candidate is presently established as a paper-worthy new algorithm.** Both contain defensible mathematics; neither should be promoted by counting established ingredients as separate inventions. CSCJ has the more distinct potential contribution: preserving a specified serial trajectory while collapsing positive moves. TDDS has the more direct collision with existing factorized query evaluation. My recommendation is **revise and falsify**, not accept either novelty claim or reject either formula.

### Findings, In Priority Order

| ID | Severity | Finding and affected claim |
| --- | --- | --- |
| C1 | P1: research blocker | CSCJ cannot compress any multi-vertex run in the singleton-initialized first sweep, not merely "often". A cached-score scalar baseline also avoids repeated full scans. The current cost comparison does not isolate the value of the jump certificate. |
| T1 | P1: research blocker | TDDS's expansion, grouped local output, signed correction representation, and aggregate pushdown have close primary precedents. A concrete specialization comparison against F-IVM is missing; naming seven scalars does not establish an algorithmic advance. |
| T2 | P1: bound/procedure gap | The oversized-moment-row fallback does not specify enough indexing or joins to justify its replay bound. Replaying the whole defect row per target tile can exceed `O(f+R2)` work and the stated mixed-I/O expression. This is repairable, not an algebra counterexample. |
| C2 | P2: evidence gap | The community probe checks expanded-graph scores, all competitors, binary search, and explicit member relabeling. It does not check compressed `x_aC` score construction, streaming extrema, direct-division code, interval commits, or event-boundary equality. |
| T3 | P2: evidence gap | The optimized `R` kernel is now genuinely exercised, but the complete resource-shaped TDDS procedure is not. Dense per-vertex `G/J` loops, direct residual triples, and unlimited small arrays do not test the claimed bulk/sparse/external schedule. |
| X1 | P2: semantic/admission gap | Boolean projection, exact type validation, integer widths, and lifecycle reservations are assumptions or proposed guards, not demonstrated implementations. Weighted Gram matrices and merely equitable partitions remain outside the respective theorems. |

Severity describes what blocks the research claim, not a production incident. **I found no counterexample to the stated CSCJ affine identities or TDDS local-count formula under their admitted assumptions.** The independently rerun small tests agree with that assessment, but do not prove the untested procedures.

The lead's same-type defect-sign observation was already incorporated during this review. The reviewed triangle note correctly says a nonempty `(vertex type, neighbor type)` bucket cannot cancel under the Boolean block model. The retained probe also checks `e_v = sum_b abs(h_vb)`. This is **resolved**, not a new defect discovered here. Signed cancellation across different types is still possible.

## Expert Lenses

The review uses four lenses: discrete optimization and exact ties; graph algebra and projection semantics; external-memory procedures and cost models; and adversarial comparison with primary literature. Mathematical deductions below are this review's derivations. Executed results are labeled separately. Literature descriptions are linked at their point of use.

## CSCJ Audit

### The Certificate Survives The Mathematical Challenge

Locus: [affine certificate](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Budgeted-Communities.md:73), [theorem](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Budgeted-Communities.md:143).

Let `g = sigma_B(0)-sigma_A(0)`, `h = sigma_B(0)-sigma_R(0)`, and `epsilon=1` if the unchanged competitor has a smaller token than `B`, otherwise `epsilon=0`. For `beta<0`, integer arithmetic gives the particularly testable form

```text
ell = min(b,
          1 + floor((g-1)/(-2*beta)),
          1 + floor((h-epsilon)/(-beta))).
```

This presupposes the first move really won, so `g>=1` and `h>=epsilon`. For `beta>=0`, `ell=b`. These are conditions for a serially valid prefix, not for the aggregate transfer merely to improve the objective. The source comparison must remain strict even when the destination has the smaller token.

The argument against all unchanged destinations reduces to their maximum score and smallest token at that score; they have identical zero slope. Including an always-available score-zero fresh singleton is essential. If `B` is newly created, it keeps its token, while the replacement fresh token is larger; equality with the replacement fresh singleton is therefore allowed. The source can disappear only after its last member's decision. These cases are consistent with the note's proof.

The existing `K_2,2` witness correctly rejects permanent twin contraction. A no-move run is also safe only because no state changes and the unvisited members have both the same certified type and current source. Equal degree, equal block totals, or an equitable partition alone are insufficient. Allowing split types is necessary, but does not itself prove long runs exist.

An independent finite arithmetic check compared the closed form above with the first failed scalar inequality for 248,040 configurations: initial source/destination/competitor scores in `[-6,6]`, `beta` in `[-4,4]`, both token-priority cases, and `b` in `[1,20]`, restricted to a valid first move. There were zero mismatches. This tests the integer crossover calculation, not graph validation or a streaming-extrema implementation.

### C1: Singleton Initialization Gives No First-Sweep Cohorts

Locus: [initialization and schedule](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Budgeted-Communities.md:51), [homogeneous intervals](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Budgeted-Communities.md:123), [cost model](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Budgeted-Communities.md:230).

**Proof:** a vertex move changes only the visited vertex's label. Earlier moves into an unvisited vertex's community do not change that unvisited vertex's label. Therefore every unvisited vertex retains its distinct initial singleton token throughout the first sweep. Every homogeneous current-label interval available at the cursor has length one. This holds even for one perfect twin type.

Executed check, unit `K_60`, one type, `gamma=1`, increasing vertex order:

| Initialization | Individual moves | Moving events | Maximum moving jump | No-move vertices |
| --- | ---: | ---: | ---: | ---: |
| 60 singletons | 59 | 59 | 1 | 1 |
| Two consecutive 30-vertex communities | 30 | 1 | 30 | 30 |

Both outputs, fresh-token states, and move counts matched full-objective scalar sweeps. Thus the warm-start example genuinely demonstrates a moving jump, but does not demonstrate an advantage on the default first pass. Charge the construction and quality of the warm start and give it identically to every comparator.

The present full-scan procedure can spend `Theta(n^2)` count-record work on a singleton clique with `t=1`: many communities remain active during many early decisions. Small `t` therefore does not imply a cheap first sweep. Reword "often gives no cohort advantage" as a theorem-level limitation and report first-sweep costs separately from later no-move grouping.

### All-Community Visits Are Not A Necessary Scalar Baseline

Locus: [all-community convention](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Budgeted-Communities.md:55).

For a positive-degree vertex and a destination with zero affinity,

```text
sigma_C = -p*k_v*K_C <= 0 = sigma_fresh.
```

A positive-volume non-neighbor community is strictly dominated by the fresh singleton. A zero-volume community ties the fresh singleton and may win by its older token. A degree-zero vertex has zero gain everywhere and stays. Consequently the same reference decision can be made by evaluating neighbor communities, the source/stay option, a fresh singleton, and the smallest-token active zero-volume community. This is a tie-preserving pruning proof for the declared nonnegative-weight profile, not permission to drop all non-neighbors indiscriminately.

**Tie witness, independently evaluated:** graph with edge `uv` of weight one and isolated `z`; partition token 0 contains `{u,v}`, token 1 contains `{z}`; `gamma=3`; next fresh token 3. On visiting `u`, `M=2`, `k_u=1`, and the source, isolate-community, and fresh scores are `-1,0,0`. The correct destination is token 1, with `DeltaQ=1/2`. A neighbor-only implementation offering only fresh as the non-neighbor alternative returns a different token/partition. Keeping the minimum zero-volume token fixes this case.

An adjacency-aware baseline with bounded neighbor tallies can therefore preserve the stated schedule without a full `C` scan on sparse graphs. Its tally spill/index costs still count. The paper currently acknowledges that pruning needs proof; this proof should now become part of the comparator specification.

### Stronger Baseline: Cache Scores Across A Type Block

The scan is also avoidable without neighbor enumeration. This is a derived comparator proposal, not a claim that a particular published implementation already does it.

For the currently visited type `a`, define unadjusted scores

```text
s_C = q*M*sum_b w_ab*x_bC - p*k_a*K_C.
source score = s_A - beta.
```

Build these scores once at the start of the contiguous type block. Every scalar type-`a` move changes only `s_A -= beta` and `s_B += beta`. Maintain a token-aware ordered score structure, with source exclusion, fresh candidates, and removal of genuinely empty communities. Then ordinary scalar decisions require two key updates and a constant number of extrema queries, not a new scan of all `x` records. Rebuild when the visited type changes.

A straightforward in-memory comparison bound per sweep is

```text
O(sum_type_blocks (x_block+C_block) + n*log(C_max+2)),
```

with `O(C)` score records, or explicitly charged external ordered-index operations if that state does not fit. This is stronger than the note's repeated-scan scalar baseline and handles fragmented source labels within a type. It must obey the same memory and live-disk cap. For the illustrative `C=1M` regime, testing whether this structure fits is mandatory rather than assuming it is too expensive.

CSCJ may still eliminate individual decisions and index updates on top of this baseline. That incremental advantage is the interesting claim. It should not be credited with saving full scans that a scalar cached implementation already avoids. Consider integrating the same cache into CSCJ, then ablate only the run certificate.

An additional ephemeral model initialized `s_C` only when the visited type changed and thereafter applied only the two score updates. Across the retained Boolean domain's 9,672 configurations, all 48,360 individual destination choices matched full-objective recomputation; final labels, fresh-token state and move counts also matched. The model selected extrema by scanning its score map. It validates the cached-score algebra and tie choices, not the proposed ordered-index complexity or external storage.

The retained small tests show moving-event reductions of about 4.47% (`16902 -> 16146`) and 11.01% (`8952 -> 7966`). Those are not speedups. Reporting grouped no-move vertices as additional novel moving-work elimination would also overlap an existing optimization, including VLouvain's batched no-mover checks.

### C2: Proof, Procedure, And Probe Are Different Objects

Locus: [community probe](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/community-triangles-Probe-Evidence.md:175).

| Claimed procedure | What the retained source actually checks | Required next test |
| --- | --- | --- |
| Scores from sparse type-community counts | Rebuilds volumes and affinities from expanded `A` and labels | Independent count-table score computation against full modularity |
| Source/best/runner-up streaming extrema | Materializes every score and checks every competitor | Stream candidates in different orders; compare selected token and maximal prefix |
| Direct integer division | Binary-searches a predicate | Run actual division implementation, especially strict-boundary/tie cases |
| Aggregate interval transfer | Relabels all members in a loop | Split/merge intervals, delete/create count cells, empty source, cursor and token state |
| Same state at every event boundary | Compares final sweep labels, moves and next token | Replay scalar prefixes to every event boundary, including exact gain receipts |
| Production exactness | Small exact `Number` values | Large integer/rational limits, malformed certificates, budget and crash boundaries |

The Boolean exhaustive domain has at most two vertices per repeated type; the weighted domain has at most three. Their maximum jumps of two and three are therefore structural ceilings, not discoveries about realistic run lengths. The `K_60` rerun extends jump length but covers only a very favorable positive-`beta` trajectory. Add long negative-`beta` runs, equality at the final accepted offset, arbitrary token permutations, fresh-community chains, and alternating labels.

Budget semantics also need precision. A scalar visit/move budget can truncate a certified prefix at the same logical position. A wall-clock budget cannot promise the same final partition across two algorithms with different speeds; the correct claim is correspondence to a scalar prefix, not identical time-budget termination. A mid-sweep stop must not produce `locally_optimal` merely because no earlier event in that partial sweep moved.

### How Small Is The Potential Novelty?

Let `k` be the vector of type degrees and `x_C` a community's multiplicity vector. The exact modularity numerator is

```text
q*M^2*Q = sum_C x_C^T (q*M*W - p*k*k^T) x_C
          - q*M*sum_a a_a*n_a.
```

The diagonal coefficient for type `a` is exactly `beta`. The affine score differences are thus discrete quadratic first differences along a repeated transfer direction. The mathematics is short and natural once the multiplicity formulation is written. The prospective contribution is an exact, identity-preserving simulation certificate plus a useful event-sensitive execution result, not a new quadratic optimization principle.

General symmetry lifting usually preserves an optimum or a relaxation; it need not preserve a particular order-dependent, tie-sensitive integer trajectory. That distinction protects CSCJ from an automatic equivalence verdict, but does not by itself establish publication-level originality. Also report how exact types are obtained: one exceptional edge per member can defeat the proposed singleton-endpoint refinement even when the defect description is small.

## TDDS Audit

### The Local Identity And Type-Batched R Are Sound In Profile

Locus: [local identity](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factorized-Triangles.md:59), [formula](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factorized-Triangles.md:78), [schedule](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factorized-Triangles.md:132).

Expanding `(K+E)^3` gives eight ordered monomials: the base plus seven delta monomials. Symmetry pairs the endpoint-reversed monomials on the diagonal, but does not turn `KEK` into `K^2E` locally. The note retains this distinction correctly. Its final scalar formula has seven displayed contributions. Clarifying these three counts would avoid making the name of the expansion sound like an additional result.

The necessary corrections also check out: `KEK` contributes `G_a-2a_a*z_v`; `EKE` needs the neighbor-diagonal subtraction; `KE^2` needs `-a_a*e_v`. Omitting any of these is not justified by zero vertex self-loops in `A`, since the type factorization temporarily introduces diagonal mass.

For each directed defect `(u,v)`, `c(v)` belongs to `D_u` by definition. Thus computing `p_u,a` only for `a in D_u` suffices; no missing target-type case was found. With resident moments and constant-cost template lookups, computing these values costs `sum r_u^2`, followed by `2f` edge emissions. This validates the mathematical mixed-term bound, not every external implementation of it.

### Resolved Sign Issue, And What Moments Still Cannot Recover

For fixed `a=c(v),b`, a nonzero defect is necessarily `1-2W_ab`. Therefore

```text
e_v = sum_b abs(h_vb)
D_v = support(h_v)
sum_u a_c(u)*E_vu^2 = sum_b a_b*abs(h_vb).
```

Unsigned degree and the diagonal correction can be derived from the type moments in this profile. Keeping independent degree counters can be a consistency check, but should not be justified by impossible within-bucket cancellation. Actual residual edge identities remain necessary for the proposed residual-triangle stage and for endpoint-specific propagation.

**Executed six-vertex witness:** take the one-type clique template `K_6` and delete either the six edges of a cycle `C_6` or the edges of two disjoint triangles. Both residuals have six edges, `h_v=-2`, `e_v=2`, `r_v=1` at every vertex, and `F=-12`. All type moments agree. Yet:

| Deleted support | Local triangle counts in the deleted support | Local triangle counts in the final graph |
| --- | --- | --- |
| `C_6` | `[0,0,0,0,0,0]` | `[1,1,1,1,1,1]` |
| Two disjoint `K_3` | `[1,1,1,1,1,1]` | `[0,0,0,0,0,0]` |

This is not a failure of TDDS, which retains `tE`. It is a concrete obstruction to replacing residual topology by `h,F,e`, and a useful regression outside the existing five-vertex exhaustive domain.

For the one-type clique/deletion special case, let `D` be the unsigned deletion graph. TDDS simplifies to

```text
t_v = choose(n-1,2) - f - (n-2)*e_v + choose(e_v,2)
      + sum_{u in N_D(v)} e_u - tD_v.
```

This elementary inclusion-exclusion baseline already eliminates explicit deletion wedges. A clique with star deletions is consequently a good kernel sanity check but a weak novelty exhibit. Compare against this specialization, not only a deliberately quadratic wedge loop.

### T2: The Oversized-Row I/O Bound Needs An Actual Join Schedule

Locus: [tiling statement](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factorized-Triangles.md:172), [mixed I/O bound](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factorized-Triangles.md:268).

The statement that oversized moment rows can be tiled/replayed with additional traffic bounded by `O(b*R2)` is underspecified. Replaying the **moment row** can have that bound. Replaying the entire **defect edge row** for every target-type tile need not.

Consider one defect-star center with `r` neighbor types and `L` leaves in each type. Every leaf has one defect neighbor. Then

```text
f = L*r
R2 = r^2 + L*r
```

If only `c` target moments fit and the full center edge row is rescanned per tile, the edge-record visits are `L*r*ceil(r/c)`. Choose `L=r^2`: this is `Theta(r^4/c)`, versus `R2=Theta(r^3)`. The family fits the Boolean sign contract, for example as deletions from a complete template. The issue is a permitted naive fallback, not a lower bound against all implementations.

An arithmetic probe of this replay plan with `c=8` gave:

| `r` | `f` | `R2` | Repeated center-edge record visits |
| ---: | ---: | ---: | ---: |
| 32 | 32,768 | 33,792 | 131,072 |
| 128 | 2,097,152 | 2,113,536 | 33,554,432 |
| 512 | 134,217,728 | 134,479,872 | 8,589,934,592 |

These are operation-count calculations, not measured disk traffic. With fixed record/page widths and arena capacity, the omitted factor is unbounded. The conservative `b*R2` term does not by itself repair a repeated edge scan for arbitrarily large `r/c`.

**Specific repair:** emit at most `H` records `(u,a,p_u,a)` as target tiles complete. Sort/merge them with directed defects keyed by `(u,c(v))`, or provide indexed edge subranges grouped by neighbor type. Then each edge can be processed once after the moment calculation. Specify the extra order/index, its build traffic, scratch lifetime, and exactly which existing sort pays for the join. Do not silently equate an unindexed row replay with that procedure.

### T3: Arithmetic Complexity Is Not Yet Whole-Procedure Complexity

Locus: [work model](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factorized-Triangles.md:238), [residual execution](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factorized-Triangles.md:277), [probe kernel](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/community-triangles-Probe-Evidence.md:88).

The current probe correctly constructs sparse `defectRows` and executes the distinct-neighbor-type `p` calculation inside the full formula. It also independently forms `E^2` for an oracle. It would be inaccurate to dismiss this as only a direct-`R` identity check. However:

- `G` is recomputed per vertex in dense type loops rather than once per type; `J` uses dense `q^2` loops rather than the advertised sparse support. This validates their algebra, not their optimized schedules.
- Signed residual triangles are obtained by direct triples. No degree-order forward-list intersection, sign retrieval, external endpoint counter, or orientation construction is tested.
- No moment row exceeds an arena. No type matrix is tiled, and no destination reduction spills. The bounded pseudocode's most important resource invariants remain unexercised.
- The stated `CPU = O(q^3+R2+f+f*delta_E+n_output)` is a kernel arithmetic/comparison count. The selected B-tree accumulator also has key-search/update work for every residual triangle, and comparison sorts have their own costs. Either label the expression as kernel-only or add the costs of the concrete index/sort procedures and nonconstant-width arithmetic.

Retaining only `O(s)` counters does not make their update traffic linear in `s`: the proposed residual accumulator still performs work proportional to unsigned `T_E`, regardless of cancellation. Nor does small retained state imply a cheap type stage: `q=1024` entails roughly a billion loop iterations for a straightforward cubic stage. The example's `f=1M,H<=2M` permits an `R2` upper bound around two billion; it supplies no observed `r_v` distribution. These are reasons to measure the cost envelope, not contradictions of the admitted upper bounds.

For a zero template, TDDS reduces to ordinary residual triangle counting. For large `q`, exact type algebra may lose before any mixed-term savings appear. For expanded input, full source parsing/sorting can dominate. For repeated full per-node output, delivery retains an `Omega(n)` cost even when the saved answer uses only bulk values and exceptions. Benchmark global-only, compressed-answer, and complete-row-output requests separately.

### Boolean Projection Is A Hard Boundary

Locus: [projection](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factorized-Triangles.md:26), [preparation](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Factorized-Triangles.md:289).

The existing projection warning is correct, not an unresolved formula bug. Three membership rows `{a,b}`, `{a,b}`, `{a}` produce shared-feature edge weights `2,1,1`; their weighted triangle product is two, but the Boolean projected graph has one triangle. No scalar overlap correction works in general. A low-rank incidence representation also does not imply few exact Boolean block types after thresholding.

The probe starts from an already constructed Boolean `A`, so it cannot validate source filtering, deduplication, feature projection, or the cost of discovering `W/E`. Add rejection/normalization tests for reciprocal records, duplicate memberships, source weights, invalid defect signs, self-pairs, and filters that invalidate a cached template. Algebra experiments using weighted coefficients must be labeled outside the simple-triangle/LCC operator. Updating an edge overlay is likewise not evidence that full projected-graph refresh semantics have been implemented.

## Nearest Primary Art

This is a claim-collision map, not an assertion that any one source contains the entire proposed algorithm. The literature descriptions below are intentionally narrower than claims of priority.

| Primary source and inspected locus | Collision | What remains unresolved |
| --- | --- | --- |
| [Yu, Srinivasan, Thomo, VLouvain, EDBT 2026, sections 4 and 5.1-5.4, Algorithms 1-3](https://openproceedings.org/2026/conf/edbt/paper-72.pdf) | Community sufficient statistics avoid dense edges; section 5.1 already skips blocks with no movers. The paper states `O(n^2*d)` first-level work and `O(n*d)` memory. | I did not find CSCJ's positive-moving affine-prefix certificate in the inspected procedure. Compare with identical objective, order, tokens and initialization. Its vector/Gram profile is not every symmetric weighted block matrix; hardware measurements are not a 4 GB baseline. |
| [Mladenov, Kleinhans, Kersting, Lifted Convex Quadratic Programming, sections 3-4](https://arxiv.org/pdf/1606.04486), and [Bui, Huynh, Riedel, Automorphism Groups of Graphical Models and Lifted Variational Inference](https://www.auai.org/~w-auai/uai2013/prints/papers/158.pdf) | Symmetry-based optimization compression and orbit-level variational formulations predate CSCJ. | These are not proofs of identical nonconvex integer coordinate trajectories with changing community tokens. The bounded symmetry/coordinate-descent search found adjacent mechanisms, not an exact CSCJ duplicate. That gap is uncertainty, not novelty clearance. |
| [Cordasco, Gargano, Rescigno, Speeding up Networks Mining via Neighborhood Diversity, section 5.1, Theorem 4](https://drops.dagstuhl.de/storage/00lipics/lipics-vol157-fun2021/LIPIcs.FUN.2021.21/LIPIcs.FUN.2021.21.pdf) | Exact triangle counting through type-graph multiplicities, with bound `O(nd^omega+n+m)` where `nd` denotes neighborhood diversity. | TDDS's zero-defect case cannot be the innovation. Compare against refining defect endpoints into types as well as counting on the original template. |
| [Shin et al., SWeG, section 3.1.4 and Algorithm 4](https://www.cs.cmu.edu/~kijungs/papers/swegWWW2019.pdf) | Lossless supernodes/superedges plus positive and negative corrections are the TDDS representation. Algorithm 4 explicitly chooses present-edge versus missing-edge encoding per block, including internal blocks. | SWeG's compression objective need not minimize `q^3+R2+f*delta_E`. An execution-aware representation choice needs its own result; a renamed summary format does not. |
| [Fan et al., Making graphs compact by lossless contraction, sections 3.2.1-3.2.2](https://link.springer.com/article/10.1007/s00778-022-00731-7) | Exact triangle answers use precomputed internal/two-in-one-supernode contributions and restore superedges as necessary, without restoring supernodes. | The inspected algorithm targets total counts; it does not by itself establish TDDS's compact local vector or its mixed-term bound. Nevertheless, an expanded-adjacency-only comparison is inadequate. |
| [Koch et al., DBToaster extended report, sections 1 and 3-5](https://dbtoaster.github.io/papers/2013-dbtoaster-report.pdf) | Recursive finite differences, signed multiplicities, multi-row results, self-join nonlinearities and view materialization are established. | Cubic delta corrections are not new. The Oxford mirror was intermittently unavailable; the project-hosted report supplied the primary text. No generated TDDS-equivalent plan was executed here. |
| [Nikolic and Olteanu, F-IVM: analytics over relational databases under updates, sections 4-6, Example 9, and section 7](https://link.springer.com/article/10.1007/s00778-023-00817-w) | View trees preserve free/grouping variables, push marginalization past joins, and handle signed updates. Example 9 explicitly represents a near-Cartesian relation as a factorized over-approximation plus a negative correction. | This is the most serious TDDS collision: translate the exact candidate into views and compare intermediates, support sizes and output encoding. Expressibility does not prove the existing compiler automatically finds the same bounded plan. |

VLouvain's printed pseudocode has details that merit checking against its code before using it as a bit-exact oracle. This review neither relies on its correctness claim as proof of CSCJ nor declares an implementation bug from PDF transcription.

### T1: A Concrete F-IVM Comparison, Not Just A Citation

The following is this review's relational restatement of TDDS, not output from an executed compiler:

```text
count2(v) = SUM_{u,w} A(v,u)*A(u,w)*A(w,v)
h(u,b)   = SUM_{v:c(v)=b} E(u,v)
F(b,c)   = SUM_{u:c(u)=b, v:c(v)=c} E(u,v)
p(u,a)   = SUM_b h(u,b)*W(a,b), restricted by D(u,a)
R(v)     = SUM_u E(u,v)*p(u,c(v)) - a_c(v)*e_v
```

Substitute the known type-factor relation for `K`, use integer payloads for `E`, retain `v` as a free key, and expand the three aliases of `A=K+E`. The `h/F` views are ordinary grouped sums; the `p` step pushes a sum below an edge join and restricts its support by a semijoin with `D`. This explains why the same `R2` bound may be obtained by a competent manually specialized factorized plan. The compact bulk/exception output must also be included in the comparison; simply requesting a flat materialized `count2(v)` table from the baseline would bias its state cost.

The revision must identify an operation, intermediate-size guarantee, external schedule, or certified representation-selection result absent from that explicit comparator. Do not argue that F-IVM is dynamic while TDDS is static: the computation is still a query on a template plus a batch of signed corrections. Conversely, do not claim F-IVM automatically supplies TDDS's resource bound without constructing its plan. The cyclic residual triangle query remains a distinct hard subproblem; acyclic-query guarantees do not transfer to it.

## Evidence And Verification

### Independent Rerun

The retained JavaScript fence was executed from the repository root without writing another file, using Node.js `v24.9.0`:

```sh
awk '/^```javascript$/{active=1;next} /^```$/{if(active){active=0;exit}} active' research_algorithms_20260920/community-triangles-Probe-Evidence.md | node
```

Exit code was zero. The output reported:

| Check | Independent rerun result |
| --- | --- |
| Triangle template/graph pairs | 65,536 |
| Local count / optimized `p` / degree / sign checks | 327,680 each; zero mismatches |
| Template-only checks | 320; zero mismatches |
| Boolean community configurations | 9,672; zero mismatches |
| Boolean moves / events / maximum jump | 16,902 / 16,146 / 2 |
| Weighted configurations / sweep comparisons | 2,000 / 3,877; zero mismatches |
| Weighted moves / events / maximum jump | 8,952 / 7,966 / 3 |

Additional ephemeral checks executed for this review: the 248,040 affine-prefix cases; 48,360 cached-score destination comparisons across 9,672 configurations; scalar-versus-fast `K_60` sweeps under both initializations; the isolated-community candidate scores; direct enumeration for the six-vertex residual witness; and the replay-plan operation counts. No machine-scale performance, disk bound, external index, crash behavior, or compiler-generated factorized plan was tested.

### Reproducible Arithmetic Probe

This dependency-free fence reproduces the independent crossover check; it writes no files. It deliberately tests scalar inequalities rather than reusing either candidate's binary-search implementation.

```javascript
let checks = 0;
for (let A = -6; A <= 6; A++)
for (let B = -6; B <= 6; B++)
for (let R = -6; R <= 6; R++)
for (let beta = -4; beta <= 4; beta++)
for (let earlier = 0; earlier <= 1; earlier++)
for (let run = 1; run <= 20; run++) {
  if (B <= A || B < R || (B === R && earlier)) continue;
  let scalar = 0;
  for (let r = 0; r < run; r++) {
    if (B - A + 2 * r * beta <= 0 || B - R + r * beta < earlier) break;
    scalar++;
  }
  let direct = run;
  if (beta < 0) direct = Math.min(run,
    1 + Math.floor((B - A - 1) / (-2 * beta)),
    1 + Math.floor((B - R - earlier) / (-beta)));
  if (scalar !== direct) throw Error(JSON.stringify({ A, B, R, beta, earlier, run }));
  checks++;
}
console.log({ affineIntegerPrefixChecks: checks, mismatches: 0 });
```

### Input Snapshot

The files were being refined by their author during review. Findings refer to the sign-corrected, sparse-`defectRows` revision, identified by these SHA-256 hashes; line links may move in later revisions.

```text
Budgeted-Communities.md
91862d8d56372f150d7b4278c65cc3622c689f1feb44927d78218ee760ee3f31
Factorized-Triangles.md
9b1911f44cbcc2bed6ca1e8161e37a5f9bce28ca2f57dcb1039f12aba110a1aa
community-triangles-Probe-Evidence.md
a0d88c5f52281c4b28211f3cb45d1c8d6e1a7ec61609ee69465771bc7db6aa02
```

## Specific Next Revisions

1. **CSCJ theorem/profile:** incorporate the singleton-first-sweep limitation and tie-preserving non-neighbor pruning; state the direct prefix formula, token lifecycle, and logical-budget contract explicitly. Keep looped multilevel and Leiden behavior outside this proof.
2. **CSCJ decisive baseline:** compare full-objective oracle, neighbor-pruned scalar, type-cached scalar, and type-cached CSCJ under identical starts/order/ties. Attribute only the remaining gain to run compression. Record distributions of moving `ell`, `beta`, `C`, `x`, interval fragmentation and per-type initialization costs.
3. **CSCJ exact procedure test:** implement only a disposable research model of sparse counts, streamed extrema, direct division and interval events; compare every event boundary and receipt to scalar execution. Test long crossovers, arbitrary tokens and failed type validation. A final-label-only test is insufficient for the strongest claim.
4. **TDDS prior-art gate:** write and cost the explicit factorized query plan above, with compact local output, and compare against it plus SWeG-style summary-aware counting, exact neighborhood-diversity counting and contraction. Separate a hand-specialized plan from an automatically generated one. Kill or downgrade the novelty claim if the same work/state bounds follow with no additional mechanism.
5. **TDDS external-schedule repair:** replace ambiguous edge-row replay by a specified type-keyed join/index; account for `H` moment records, `2f` edge records, all sort orders, and overlapping scratch. Distinguish kernel arithmetic from index/sort CPU. Force tiny arenas to exercise the branch that the current probe never enters.
6. **TDDS falsification suite:** add the `C_6`/two-triangle witness, large same-type defect stars, distinct-type stars, residual cliques, fragmented endpoint IDs, empty/complete templates, and illegal weighted projections. Test optimized `J`, type-once `G`, signed forward intersections, and exact output merging separately.
7. **Shared publication gate:** exhibit at least one nontrivial input family with an explicit advantage over the strongest factor-aware comparator, and a failure family where the planner declines. Then measure source-to-complete-output and refresh on the actual memory/disk target, including preparation, retained readers, failed fast paths and slow consumers. Do not substitute selected payload sizes for a run.

## Candidate Approaches

Three defensible next directions, not three additional claimed algorithms:

- Strengthen CSCJ into a general exact repeated-coordinate trajectory certificate, with an event-sensitive data structure and a separation from cached scalar execution. The main risk is that useful moving runs do not survive realistic initialization and type discovery.
- Treat TDDS as a specialization of factorized view evaluation and seek a proven support/I/O advantage for compact local output. The main risk is equivalence to a straightforward existing-plan specialization.
- Investigate cost-aware template selection/admission against both arithmetic and full workflow costs. This could address the shared practical weakness, but currently has no theorem or probe; ordinary heuristics and a new acronym would not suffice.

## Chosen Thesis

Keep CSCJ as the stronger **research hypothesis**, contingent on the cached-scalar comparison and realistic moving-run evidence. Treat TDDS as a sound **derived kernel/specialization** until its exact F-IVM comparison and external schedule demonstrate a contribution beyond established rewriting. Neither designation is a deployment recommendation.

## Final Synthesis

The actual local formulas withstood this review. The important negative results concern what those formulas do not buy: CSCJ has no default first-sweep cohort compression and currently pays avoidable scan costs; TDDS's attractive arithmetic is very close to existing factorized algebra and its tiled edge-emission procedure is not fully specified. The test suite supports small exact identities, not the complete proposed resource-shaped implementations.

The strongest next paper claim must be one measurable new mechanism, with a precise comparator and a falsifiable parameter regime. Seven renamed known methods, correct algebra alone, or generous novelty disclaimers would not meet the stated objective.

## Open Questions

- Does CSCJ eliminate enough positive decisions after the first sweep to beat type-cached scalar execution once input validation and output are charged?
- Does a prior exact symmetry-aware coordinate method already implement the same run/crossover certificate? This bounded search did not settle that question.
- Can TDDS's local bulk/exception representation and `R2` schedule be reproduced by an explicit F-IVM specialization with the same live state and external traffic?
- Which authoritative datasets actually provide small useful templates, rather than merely small feature dimension or approximate communities?
- Can either candidate demonstrate the required end-to-end gain on the physical target without changing the problem, warm start, output contract, or budget between comparators?

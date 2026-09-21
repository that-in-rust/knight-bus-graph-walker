# PageRank On Native Incidence: Complete Certified Cost

Date: 2026-09-20. A01 research experiment. This is a test of practical eligibility and strong controls, not a new inverse identity, a Neo4j benchmark, or a completed seven-family innovation claim.

## Premise Check

The [factor-defect proposal](Factor-Defect-PageRank.md) can eliminate entity-indexed iterative vectors. That does not establish either lower complete-job RAM or faster results. It can also suffer slow convergence on cancellation-heavy factorizations. The next useful question is whether a naturally supplied incidence source benefits after giving the incumbent the SAME factors, a one-pass schedule, an ordinary Krylov solver and an admitted dense solve.

This study deliberately retains a negative outcome if those controls remove the advantage. The earlier two-node cancellation adversary must not be mistaken for representative binary membership data.

## Expert Lenses

- Numerical linear algebra: solve the original stationary equation, including isolated vertices and rounded output.
- Storage execution: count input construction, factor-row passes, solver vectors, output and certificate workspace.
- Product workflow: distinguish a faster reusable query from a cheaper first complete result.
- Skeptical comparison: do not attribute factor compression, ordinary CG or known elimination to a new algorithm.

## Candidate Approaches

1. Ordinary vertex-state power iteration over supplied factors, without expanding shared-membership edges.
2. A fused vertex/factor schedule, borrowing streaming dataflow's retained sufficient statistics to remove the separate factor-gather pass.
3. Factor-only iteration, with optional diagonal Jacobi splitting from classical numerical analysis.
4. A symmetric reduced solve: matrix-free CG or, when explicitly admitted, a dense capacitance matrix. These are existing methods, not new contributions.
5. A separate interval-arithmetic audit of the published file, so iteration stopping heuristics cannot silently redefine correctness.

## Chosen Thesis

**Compare complete certified jobs on a natural factor source before promoting a state-saving theorem into a product or paper claim.** All eight modes use identical input semantics and the same final certificate. The candidate must beat the strongest applicable control on a stated resource, not just a weak expanded graph implementation.

## Source And Permission

The source is the GroupLens MovieLens 100K rating file: [dataset landing page](https://grouplens.org/datasets/movielens/100k/), [original README and research-use conditions](https://files.grouplens.org/datasets/movielens/ml-100k-README.txt), [u.data](https://files.grouplens.org/datasets/movielens/ml-100k/u.data). We use only anonymized rater/movie membership, not demographic files. Ratings and timestamps are deliberately ignored. This does NOT evaluate recommendation quality.

Attribution: F. Maxwell Harper and Joseph A. Konstan, 2015, *The MovieLens Datasets: History and Context*, ACM TiiS 5(4), Article 19, [DOI 10.1145/2827872](https://doi.org/10.1145/2827872). No affiliation or endorsement is implied.

The README permits research with attribution but restricts redistribution and commercial use. Raw input, the prepared incidence artifact and individual output vectors stay outside the repository under `/tmp`; retained receipts contain hashes and aggregate measurements only. Commercial deployment would require a separately suitable dataset/license.

## Exact Projection

Movies are the N vertices; raters are the F factors. Binary B[i,f] is one if the source contains the membership. Duplicate memberships are rejected, not silently summed. Input IDs are one-based and output rows follow zero-based movie order. The movie universe extends through the maximum input movie ID, preserving holes as isolated vertices. Factors with fewer than two members are removed, without deleting vertices.

```text
A = B B^T - diag(q)       q_i = number of retained memberships of i
d_i = sum_{f containing i} (size_f - 1)
```

Thus edge weight is the number of shared raters, the graph is symmetric and loop-free, and degrees are accumulated in exact integers before floating conversion. The sum of degrees counts weighted directed edge mass, NOT distinct edges.

The source hash is `06416e597f82b7342361e41163890c81036900f418ad91315590814211dca490`; raw bytes: 1,979,173. The compiled source has 1,682 vertices, 943 factors, 100,000 memberships, no isolates and no pruned factors. Group sizes range from 20 to 737; the largest entity row contains 583 memberships. Its weighted directed edge mass is 20,100,812. The prepared row file is 420,216 bytes with hash `9e4acc25fc0b988f2a06d4f0e793221e7133c1ed8fb02d71096c78a73e45bdec`.

### Why The Earlier Heavy-Release Case Does Not Apply

If every retained group has size at least s>=2, then

```text
d_i >= (s-1) q_i
q_i/d_i <= 1/(s-1)
beta <= a*s/(s-1+a).
```

This follows term by term in the exact degree sum. In particular q_i>d_i is impossible on this binary projection. The earlier heavy-diagonal-release theorem is useful for a broader representation class, not automatically here. On this input the actual maximum ratio is 1/33, giving beta approximately 0.85376662 at a=0.85. This is close to the ordinary power contraction upper bound 0.85; neither upper bound predicts actual iterations.

## Original Operator And Isolates

The exact target is normalized stationary PageRank with a=17/20, personalization p, and dangling mass sent to p. Queries are uniform p, unit mass on movie index 0, and unit mass on index N-1. These choices are fixed before timing. Tolerance is global L1 error <=1e-10, not GDS's finite-iteration score-change tolerance.

For this symmetric projection, d_i=0 implies a truly isolated vertex. If pZ is personalization mass on isolates, eliminate the dangling scalar exactly:

```text
bEffective = ((1-a)/(1-a*pZ)) p
x_i = bEffective_i                          on isolates
(D + a Q - a B B^T) y = bEffective           on active vertices
x = D y                                    on active vertices
```

All modes receive this same simplification. Do not use it on the general directed graph where a dangling vertex can have incoming edges.

Let K=D+aQ on active vertices. The reduced matrix is

```text
C = I - a B^T K^-1 B
C h = B^T K^-1 bEffective
x_i = d_i (bEffective_i + a (B h)_i)/(d_i+a*q_i).
```

C is symmetric positive definite. Its eigenvalues lie in [1-beta,1], so the corresponding condition-number bound is 1/(1-beta). Symmetry makes ordinary CG a required control, not an innovative solver to claim as ours. The connection to known capacitance elimination is documented in [the prior-art comparison](PageRank-Prior-Art-Comparison.md).

## Eight Frozen Methods

| Mode | Retained solve representation | Work and strong-control purpose |
| --- | --- | --- |
| power | One vertex vector plus gathered factors | Two row scans per iteration; updates vertex values in place after factor gathering |
| fused | Vertex vector plus old/new factor gathers | One row scan per iteration; same ordinary vertex recurrence, no full edge expansion |
| filepower | Old/new factor gathers; vertex scores in two alternating files | Same fused recurrence without a resident vertex vector; charge every score read/write and initial score file |
| reduced | Old/new factor vectors | One row scan per residual check; reconstructs and gathers without retaining vertex scores |
| jacobi | Factor vectors plus diagonal | Same reduced original residual; ordinary diagonal splitting, not a new method |
| cg | Factor Krylov vectors | One row scan per matrix application; ordinary symmetric reduced CG |
| vertexcg | Vertex Krylov vectors and factor gather | Two row scans per degree-scaled symmetric original matrix application; ordinary CG is not exclusive to the reduced system |
| dense | Explicit F-by-F capacitance matrix | Dense baseline admitted only at F<=1024, before its allocation; matrix construction, Cholesky and triangular solves charged |

The fused recurrence is `xNew_i=bEffective_i+a*(sum_f h_f-q_i*x_i/d_i)`, followed immediately by `hNew_f += xNew_i/d_i`. Old h stays unchanged during the row pass. Entity values can be overwritten in place because other rows read old factors, not those values. Isolates use the analytic value.

For reduced power, reconstruct X(h) and gather hNext in one pass. Test the weighted original-residual heuristic for h; when it passes, publish X(h), not X(hNext). Jacobi uses the same original residual rather than its update difference. CG's recursive residual is a heuristic only. Every method must pass the independent rounded-output certificate below before returning success.

The ordinary vertex CG system is `L=I+a*Q*D^-1-a*D^-1/2*B*B^T*D^-1/2`, with RHS `D^-1/2*bEffective` and publication `x=D^1/2*v`. Its recursive residual screen uses `sqrt(sum_i d_i)*||r_v||_2/(1-a)`. This is a conservative stopping conversion, followed by the same independent final certificate. This implementation gathers then applies in two passes; a further retained-factor recurrence could change that schedule and is not implemented. The experiment does not establish optimality over every possible factor-aware solver.

File-backed power proves that an ordinary method can also have no resident N-sized iterate. The factor-only design avoids its repeated changing-score traffic, not a universal necessity for ordinary PageRank to keep all vertex scores in RAM. The file-backed control uses the filesystem cache; no durable fsync or recovery guarantee is benchmarked.

The prototype is [probe_native_incidence_pagerank.py](experiments/probe_native_incidence_pagerank.py). Binary rows contain an exact u64 degree and u32 count followed by u32 factor IDs. The decoder retains one full row; it is NOT the proposed arbitrary-degree chunked production reader. The builder also retains source groups and entity row lists. Those restrictions preclude a physical 4 GB scalability claim.

## Rounded-Output Certificate

The final binary file contains one IEEE f64 score per original vertex. The certifier reads each value with `Decimal.from_float`, preserving the exact real value of those published bits. It uses distinct 50-digit floor/ceiling contexts; never an unqualified rounded floating residual.

1. First row/output scan: enclose x_i/d_i and gather lower/upper factor sums. Accumulate dangling mass with directed rounding.
2. Second scan: enclose every component of the ORIGINAL residual `r=(1-a)p+aP*x-x`, including diagonal cancellation and dangling redistribution.
3. Sum `max(abs(rLower_i),abs(rUpper_i))` upward, divide upward by exact 1-a, and compare with the requested tolerance.

The proof uses stochastic P and `||(I-aP)^-1||_1<=1/(1-a)`. Interval operations include the cancellation, so the certificate concerns the actual output file, not only an ideal reduced solution. It stores two F-sized Decimal arrays, not an N-sized residual array. Decimal objects, precision, Python runtime and row buffers are real memory costs. A 50-digit computation is not free or automatically faster than ordinary residual evaluation.

The certificate trusts the correctly compiled operator; it is not a proof that arbitrary corrupted degree records match some external source. Here exact integer construction, source/store hashes, and independent expanded-operator tests link the source semantics to that operator.

## Verification Before Public Timing

The first six tests failed because the probe did not exist, then passed after implementation. A further strong-control test first failed on missing file-backed power, then passed after both extra controls were implemented. The suite constructs all 128 incidence families on three vertices, with uniform and one-node personalization, and runs all eight methods: 2,048 complete answers. An independent Fraction Gaussian elimination over expanded original adjacency provides the oracle; it does not call the factor solver. Every measured L1 error must lie below the Decimal certificate and the requested tolerance.

Further checks cover removed singleton factors with preserved isolates, duplicate rejection, known degrees, the group-size contraction bound, dense refusal before output/matrix construction, deliberately wrong output rejection, truncated output rejection, and signed rounded vectors with tiny/large magnitudes. These small finite checks support the implementation but are not a proof of universal numerical correctness or a scalability result.

## Frozen Public Protocol

Run all eight methods for all three prescribed personalizations, three fresh worker processes each: 72 complete results. Run serially after the independent review and all tests are terminal. Rotate method order by repetition. Pin common BLAS/OpenMP thread environment caps to one. Record process wall time separately from internal stage time and record maximum worker RSS with the platform's correct units. Do not claim an isolated host or verified single-thread BLAS behavior from environment settings alone.

For each result retain preparation, solve, output and interval-certificate durations; exact row-pass and membership counters; logical bytes; complete-output hash; final certificate; dense pair-update count; worker RSS. Common source construction is a separate fresh child and is charged once for a reusable artifact, or once per cold first-job comparison. Input download is a network acquisition cost outside these local build timings.

No benchmark bypasses correctness on a hard query. Failure to reach the heuristic or pass certification is a reported failure, not a faster result. These are cached, tiny Python experiments on the local machine. They cannot establish Rust speed, disk throughput, cloud tail latency, arbitrary graph eligibility or Neo4j/GDS performance.

## Evidence And Verification

The [completed independent mathematical/prior-art review](PageRank-Native-Incidence-Review.md) supports the isolate elimination, spectral bound, Jacobi convergence and conceptual interval certificate. It did not inspect implementation code. Its strongest-control advice motivated file-backed ordinary power, vertex CG and Cholesky before any public timing. All eight focused tests pass in 4.697 seconds, including 2,048 exact-oracle answers. Test and reviewer processes were terminal before public timing.

The [retained public receipt](PageRank-Native-Incidence-Results.json) records all 72 completed worker executions. Every complete file passes the original-operator interval certificate; the largest certified L1 bound is approximately 1.140e-11, below 1e-10. Within each method/query, all three repeats have identical iteration counts, row counts, output hashes and certificate bounds. These repeats are NOT 72 independent graph datasets: there is one input and three distinct queries.

### Complete Method Comparison

Time below is the **sum of the three per-query median complete-method times**; each includes query preparation, solve, full output and certification. It excludes the separately reported common source build and child startup. Memory is the median maximum RSS across that method's nine worker processes, in decimal MB, NOT a memory reservation or whole-machine measurement.

| Method | Total ms | Change versus fused | Checks/iterations: uniform, first, last | Noncertificate row scans | Worker peak RSS MB |
| --- | ---: | ---: | --- | --- | ---: |
| Ordinary two-pass power | 972.301 | +34.40% | 18, 17, 18 | 37, 35, 37 | 20.759 |
| Fused ordinary power | 723.446 | baseline | 18, 17, 18 | 19, 18, 19 | 20.709 |
| File-backed ordinary power | 732.584 | +1.26% | 18, 17, 18 | 19, 18, 19 | 20.840 |
| Factor-only reduced power | 3,482.818 | +381.42% | 108, 116, 116 | 110, 118, 118 | 20.955 |
| Factor Jacobi | 3,790.871 | +424.00% | 128, 125, 121 | 130, 127, 123 | 20.972 |
| Factor CG | 564.688 | -21.95% | 11, 11, 11 | 14, 14, 14 | 20.726 |
| Vertex CG | 655.893 | -9.34% | 10, 10, 10 | 25, 25, 25 | 20.922 |
| Dense capacitance/Cholesky | 527.226 | -27.12% | direct solve | 3, 3, 3 | 56.377 |

The factor-only power method is **4.81x as slow** as fused ordinary power, despite fewer iterate coordinates. Factor CG is 13.91% faster than the implemented vertex CG. Dense is fastest here, but its 943-by-943 matrix alone contains `8*943*943 = 7,113,992` bytes, before factorization copies, workspace and runtime.

### Stage Costs And Lifecycle Scope

The measured source build takes 55.624 ms inside its worker, including parsing, construction and source/store hashing; child wall time is 80.555 ms. The row-build substage is 13.090 ms and is already included, not an additional charge. Builder maximum RSS is 24.936 MB. Raw data download is separately excluded.

| Method | Preparation ms | Solve ms | Output ms | Certificate ms | Child wall ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fused | 25.626 | 548.280 | 0.828 | 148.212 | 793.978 |
| File-backed power | 25.045 | 560.214 | 0.539 | 146.707 | 802.298 |
| Reduced power | 35.443 | 3,279.288 | 21.334 | 147.549 | 3,554.693 |
| Factor CG | 43.634 | 352.146 | 21.388 | 147.356 | 636.157 |
| Vertex CG | 25.167 | 471.492 | 12.962 | 145.841 | 726.776 |
| Dense | 16.171 | 340.812 | 21.800 | 147.953 | 608.858 |

Each stage is separately summed over per-query medians, so its columns need not add exactly to the median total. Certification accounts for approximately 26.10% of the factor-CG total. It cannot be omitted from the product comparison. Original/fused modes already retain vertex scores; factor-only methods pay a final reconstruction pass.

For a reusable three-query session, add the common build once. For three independent first jobs, add it three times. The current driver rebuilds the prepared artifact once, but dense C is reassembled for every query; reusing an admitted dense factorization across personalizations is an additional strong control, not priced here. Its advantage could grow under that different workflow.

Worker RSS includes interpreter, decoder and interval certificate, but excludes the coordinator and does not separately account for filesystem cache or other processes. Non-dense worker ranges overlap around 20.6-21.2 MB. There is **no demonstrated non-dense whole-process RAM reduction**. The builder itself peaks above those query workers; a query-only array saving would not establish a smaller complete workflow even if that array saving were large. Do not add or subtract separate-process RSS values as though they proved a physical-memory bound.

### I/O And Preparation Are Not Free

Across one execution of each of the three queries, complete logical reads are 26,134,128 bytes for fused, 26,847,296 for file-backed power, 147,996,768 for reduced power, 20,251,104 for factor CG and 34,118,232 for vertex CG. These counts include both row/output certificate scans. File-backed power writes 753,536 score bytes including initialization and all iterations; the other listed modes each write 40,368 final-answer bytes. These are logical cached-I/O counts, not SSD physical traffic.

Dense assembly performs 16,807,190 factor-pair updates per query, despite only 100,000 source memberships. It incurs quadratic matrix storage and Cholesky work that simple row-read totals conceal. Python startup, NumPy import and dense-library workspace contribute to its measured cost/RSS.

### Numerical Failure Diagnosis

An untimed replay of the uniform query inspected the raw reduced reconstruction after successive updates. After ten updates, all 943 factor differences are positive. Its total output mass is about 0.999926761732; the missing mass is 7.3238268e-5, essentially the weighted original-error bound at that point. At thirty updates the mass deficit is still 2.8811699e-6 and every difference remains positive.

This is not evidence that a loose triangle-inequality certificate alone costs one hundred iterations: in this phase the residual contributions have the same sign. The slow mode contains a real mass deficit introduced by the reduced diagonal splitting. The [mass-conservation correction note](PageRank-Mass-Conservation-Correction.md) derives an explicit diagnostic identity and a candidate correction using established coarse-space projection. That follow-on is not part of these frozen timings. Its later [implementation and paired results](PageRank-Mass-Correction-Experiment.md) reduce checks to 17-18 while matching, not beating, ordinary power's time; CG and paid reused-dense controls remain stronger on time in that separate study.

### Reproduction And Hashes

The following command assumes the separately licensed source is already at the external cache path and uses the bundled Python with NumPy. It runs workers sequentially and writes only the aggregate receipt in the repository:

```sh
/Users/amuldotexe/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 \
  research_algorithms_20260920/experiments/probe_native_incidence_pagerank.py \
  --source /tmp/knight-bus-ml100k-u.data \
  --store /tmp/knight-bus-ml100k-rows.bin \
  --receipt research_algorithms_20260920/PageRank-Native-Incidence-Results.json \
  --repeats 3
```

- Probe SHA256: `d09558f63a906ccfdb859f7db6272caf1110474182b96fa7de088dfc3db64b65`.
- Tests SHA256: `d26e1e8900a85baf51881422cc1fe3fda4a3148aaecb758bdfc4edb65b2090d9`.
- Public receipt SHA256: `58035cea3ab4ddc74969ebf052f47b5d8c9301457b2535f934aac32c3c3a11d6`.
- Independent math/art review SHA256: `7eb785c26dd87e77ed7b69898bffb0efa229e0d6f20c4f45dac7c5b066118b41`.

The host is macOS 15.3.1 arm64; bundled Python is 3.12.14 and NumPy 2.3.5. Only the dense worker imports NumPy. Timing is descriptive for these runs; three repetitions do not supply a population confidence interval or tail-latency claim.

## Final Synthesis

The source supplies real native factors, but only a 1.78x entity/factor dimension ratio. Every experimental mode already avoids materializing the weighted clique edges. The differentiating question is remaining solve/certificate state and complete time, not a misleading comparison against expanding twenty million weighted edge contributions.

The practical result is **solver selection matters more than fewer coordinates alone**. Factor power/Jacobi lose badly; known CG and admitted dense elimination are useful. The experiment provides an encoded native representation, actual final-file numerical certification and full small-worker timing evidence. It does not establish a new graph algorithm, physical 4 GB scalability, or a Neo4j speed/RAM improvement.

## Open Questions

- The later mass-correction study answers the immediate iteration question positively, but establishes no overall superiority over ordinary power, CG or reused dense solving. Can the [proposed streaming publication certificate](PageRank-Streaming-Publication-Certificate.md) now improve the complete resource frontier, with the same path supplied to strong controls?
- Can a meaningful source/query-size regime reduce measured process memory, not just a small selected array, when certification/runtime overhead dominates?
- Which native sources admit much smaller F/N, and does their source/build/refresh cost preserve the benefit?
- Can a genuinely new joint representation, solver and certification result improve over known capacitance and factor-aware methods? A positive timing alone would not prove that priority claim.

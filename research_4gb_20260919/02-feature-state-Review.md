# Independent Feature-State PageRank Review

Date: 2026-09-20 local. Reader02. Review of `Feature-State-PageRank.md`, physical lines 1-222, 18,885 bytes at the reviewed snapshot. Full contiguous bounded reads: 1-200 and 201-222; numbered verification reread 1-222. No linked document was recursively read for this review. Existing PRD06, transferred PRD04 and radar-middle scopes remain complete; the raw-tool scope was checkpointed at page35/P941 and resumes after this review.

## Verdict And Findings

No counterexample to the exact stationary elimination or the displayed residual bound was found **within the declared binary-incidence, symmetric shared-feature-count, no-self-edge contract**. The mathematical proposal is credible. It is not yet a certified floating-point solver or an admitted four-GB lifecycle. These are separate conclusions.

1. **Certificate/output clarification required:** finite reconstructed iterates generally do not sum to one. Do not normalize a candidate output and reuse its old certificate. A three-entity exact counterexample below violates that interpretation. Lines 45 and 127 acknowledge normalization/numerics, but the output contract at 73-82 and 113-125 should explicitly say that the checked reconstruction is returned unchanged, or that a new bound covers normalization. This is an interface hazard, not a contradiction in the stated raw reconstruction formula.
2. **Production-certificate blocker, already acknowledged:** lines 113-127 are an exact-arithmetic certificate. Error in p normalization, p0/c, degrees/counts, w, b, stored states, row reductions and reconstruction must be propagated outward. The 1e-10 testing allowance in lines 199-204 is not such a proof. A small negative update or rounded-small difference cannot simply be declared exact zero.
3. **Capacity/performance admission blocker, already acknowledged:** lines 139-179 name a plausible prepared portfolio and bounded-build operations, but do not specify simultaneous whole-process RAM pools, sort/merge scratch peaks, complete build passes or a measured iteration service rate. Sixteen MB is selected mutable vector payload, not a four-GB process proof. The favorable example is not a general 1B-entity admission rule.
4. **Iteration-bound strengthening, not an algebra defect:** a weighted feature L1 contraction permits a rigorously bounded newer-state return without another incidence scan. Lines 125-127 are right to require another *justified bound*; a fresh residual scan is not mathematically necessary. The proof below also tightens the zero-start statement at line 123 from "may be tight" to equality in exact arithmetic.
5. **Explicit empty-state branch needed:** after pruning, all-dangling input has F=0. Return x=p with c=1; do not evaluate the maximum over an empty active set in lines 99-105. Also a=0 returns p directly. These cases do not invalidate the formulas when interpreted with empty sums.
6. **Deadline wording needs a tolerance:** at the example's assumed 500,000,000 B/s, a 1e-8 current-state certificate from zero requires 114 row scans in the specified iteration, or 4,560 seconds of row service alone. The "hourly batch" suggestion at line 171 is not yet a one-hour SLA. This conditional arithmetic is not a measured result or an impossibility claim about better solvers.

## Exact-Arithmetic Arguments That Pass

### Operator And Singleton Pruning

Lines 23-43 correctly define A=BB^T-Q with Q=diag(q), not sign(BB^T)-I. A singleton column e_i contributes e_i e_i^T to BB^T and the same diagonal contribution to Q; both cancel on removal. Empty columns also contribute zero. Thus A and weighted degree d are unchanged, provided q is recomputed and the explicit entity universe retains newly featureless entities. Singleton membership provenance remains a different query contract.

After pruning, every retained feature has k_h>=2. Hence d_i=sum_h B_ih(k_h-1)>=q_i for every active entity. Two distinct feature columns with identical memberships are allowed: they represent two shared features and must not be deduplicated into one feature merely because their columns coincide.

### Dangling Mass And Elimination

Lines 49-80 pass. Symmetry makes zero weighted degree equivalent to no incoming contribution. With z=c*p0 and c=(1-a)+a*z, c=(1-a)/(1-a*p0); the denominator is positive for 0<=a<1. For active entities, C=D+aQ has positive diagonal, y=D^-1 x and C y=c p+a B s. Substitution s=B^T y gives s=c b+M s, where b=B^T C^-1 p and M=a B^T C^-1 B. Reconstruction is x=D C^-1(c p+a B s) on active entities and x=c p on dangling ones.

There is no hidden requirement that B have full rank. D-aA=(1-a)D+a(D-A) is positive definite on the active subspace; the graph can be disconnected and personalization can be sparse. If p0=1, c=1, active forcing is zero and the answer is p. If all columns vanish, the feature system is empty rather than singular in a harmful sense.

### Convergence And A Stronger Norm Bound

Lines 99-109 pass. The nonzero spectra of aB^T C^-1 B and aC^-1 BB^T coincide, including when B is rectangular. The latter row sums are beta_i=a(d_i+q_i)/(d_i+a q_i)<1. Let beta=max beta_i on the active set. After pruning beta<=2a/(1+a), so the fixed-point iteration converges. The SPD argument gives an independent nonsingularity check.

There is additionally a directly relevant induced norm. For retained-feature counts k=B^T 1, define ||v||_(1,k)=sum_h k_h |v_h|. Every k_h is positive. For each h:

```text
(k^T M)_h
 = sum_i B_ih * a*(d_i+q_i)/(d_i+a*q_i)
 <= beta * k_h.

Therefore ||M v||_(1,k) <= beta * ||v||_(1,k).
```

This holds for signed feature differences as well as nonnegative ones. It converts the row-ratio bound into a contraction for the exact norm used by the proposed certificate, rather than leaving it only as an asymptotic spectral observation. A universal pruned beta=2a/(1+a) can be used without retaining all entity ratios; a sharper scalar maximum can be accumulated during a row pass. Computing either bound rigorously in floating arithmetic remains separately chargeable.

### Residual And State Indexing

Lines 113-123 pass, even when the reconstructed finite x is not normalized. For current s, let next=c b+M s and y=C^-1(c p+a B s). Then B^T y=next. The prescribed dangling part is c p, so its mass is c p0 and the teleport/dangling contribution is exactly c p. Consequently:

```text
r = c*p + a*A*y - x
  = c*p + a*B*next - C*y
  = a*B*(next-s).

E(s) = a/(1-a) * ||next-s||_(1,k)
     >= ||x(s)-x_star||_1.
```

P with the prescribed dangling redistribution is nonnegative column-stochastic. Thus ||(I-aP)^-1||_1<=1/(1-a), which proves the second inequality independently of a probability-mass assumption on the finite iterate.

If delta=next-s, the subsequent difference is M*delta. Therefore **E(next)<=beta*E(s)**. Returning the newer reconstruction with this upper bound is valid in exact arithmetic and avoids an extra certification scan. It is not valid to replace beta by a: beta can exceed a. Ordinary state swapping without documenting which bound is attached to which state remains an avoidable contract error.

Starting at s_0=0 with nonnegative p, all feature updates are monotone and x(s_t)<=x_star componentwise. Then r_t>=0 and the triangle inequality in the feature residual is an equality. Since 1^T r_t=(1-a)(1-1^T x_t):

```text
E(s_t) = 1 - sum_i x_i(s_t) = ||x_star-x(s_t)||_1.
```

This exact equality applies to the zero-start exact iteration, not arbitrary warm starts, extrapolated states, or silently rounded implementations. It explains why an apparently good ranking shape can still fail the specified absolute-mass certificate.

## Counterexamples And Rational Fixtures

### Two Entities, One Shared Feature

B=(1,1)^T, a=1/2 and p=(1,0) give d=q=1, k=2, w=2/3, c=1/2, b=2/3, K=4/3 and M=2/3. The stationary feature value is s_star=1 and x_star=(2/3,1/3).

At s_0=0, next=1/3, x_0=(1/3,0), residual=(1/6,1/6), and E_0=2/3 exactly. At s_1=1/3, x_1=(4/9,1/9) and E_1=4/9. The valid factor is beta=2/3. The tempting substitute a*E_0=1/3 is smaller than the actual next-state error 4/9. This also checks that diagonal subtraction, which removes self transitions, is essential.

### Normalizing The Certified Output Can Fail

Add an isolated third entity: B=(1,1,0)^T, a=1/2, p=(1/10,0,9/10). Then p0=9/10, c=10/11 and:

```text
x_star = (4/33, 2/33, 9/11)
x(s_0) = (2/33, 0,    9/11), with total mass 29/33
E(s_0) = 4/33, exactly the unnormalized output error.

normalized x(s_0) = (2/29, 0, 27/29)
its true L1 error = 72/319 > 4/33.
```

Hence a normalized export cannot inherit E unchanged. For nonnegative x with positive total mass and known ||x-x_star||_1<=E, a simple general alternative is ||x/sum(x)-x_star||_1<=2E, using |1-sum(x)|<=E. A sharper normalization-specific bound is possible, but must be stated and numerically accounted for. Streaming normalization also needs a known mass or an additional paid pass/buffer strategy.

### Why The Symmetric Restriction Matters

For the directed edge 1->2, a=1/2 and p=(1/2,1/2), ordinary dangling-corrected PageRank is (2/5,3/5). Applying the proposed p0=1/2 correction blindly would set c=2/3 and dangling x_2=1/3, which is wrong because that dangling vertex has incoming flow. This is an excluded input, not a counterexample to the source's restricted claim at line 43.

### Counts Are Not Binary Shared-Any Edges

Take memberships h1={1,2}, h2={1,2}, h3={1,3}, a=1/2, uniform p. Count-weighted adjacency has A_12=2, A_13=1 and stationary x=(4/9,17/54,13/54). The binary shared-any star has x=(4/9,5/18,5/18). Their L1 difference is 2/27. Lines 25 and 184 correctly exclude conflating them. Pair-specific eligibility, correlations, time predicates, and arbitrary weights similarly require a new operator proof, not just a different filter flag.

## Billion-Entity Accounting

The constructed example at lines 147-169 is internally consistent. A simple 2,000-regular graph on 1,000,000 feature vertices has 1,000,000,000 edges/entities. Each entity has q=2, each feature k=2,000, and d=3,998. Distinct feature-graph edges share at most one endpoint, so the 3,998 projected neighbors are distinct. The explicit directed u32 neighbor payload is 15,992,000,000,000 bytes; this is not an actual measured compressed GDS representation.

| Named prepared component | Decimal bytes |
| --- | ---: |
| Packed entity rows, 12N+4I | 20,000,000,000 |
| Original u64 entity IDs | 8,000,000,000 |
| Score plane | 8,000,000,000 |
| Feature k and b columns | 16,000,000 |
| Two feature checkpoint vectors | 16,000,000 |
| Total selected portfolio | 36,032,000,000 |

This leaves 13,968,000,000 bytes below the 50,000,000,000-byte prepared cap before other named costs. A persisted dense f64 p adds 8 GB, leaving 5.968 GB. Keeping a separate 16 GB ID-plus-score export with the selected portfolio reaches 52.032 GB and fails that cap before metadata. Two unshared generations reach 72.064 GB. The source already warns about these categories; its "plausible allowance" wording is appropriately weaker than a complete fit.

Two resident feature vectors are 16,000,000 bytes. A full RAM schedule must additionally cap decoding and row buffers; external-ID sort/join state; count aggregation; merge fan-in and I/O buffers; personalization processing; b/k streaming; numerical-error state; runtime/native allocations; page-cache residency; output buffering; and any concurrent jobs or retained mappings. Packed serialized rows do not prove a native in-memory struct is 12 bytes. Very high-degree rows must pay the documented reread or buffer cost. Build scratch, source retention, crash recovery and refresh coexistence must be specified per phase, not inferred from final prepared bytes.

### Conditional Iteration And I/O Count

For this regular example with a=17/20, every row ratio is beta=34000/39997, approximately 0.8500637548. With zero initialization and normalized active personalization, weighted difference mass contracts by exactly beta on each iteration. Thus for the source's current-state certificate, E(s_t)=beta^(t+1), independent of whether that personalization is uniform, sparse or nonuniform. This is a property of this regular constructed model, not arbitrary inputs.

| Requested L1 bound | First current-state index t | Update/certificate row scans | Row bytes | Service seconds at assumed 500 MB/s |
| --- | ---: | ---: | ---: | ---: |
| 1e-6 | 85 | 86 | 1,720,000,000,000 | 3,440 |
| 1e-8 | 113 | 114 | 2,280,000,000,000 | 4,560 |
| 1e-10 | 141 | 142 | 2,840,000,000,000 | 5,680 |

The thresholds were checked by exact integer comparisons of powers of 34000/39997. This schedule excludes the initial b-construction scan, final reconstruction/ID delivery, checkpoints, b/k reads, build and non-I/O work. The newer-state beta-bound permits saving one certificate scan for the same output target. Other initializations or solvers can change these counts: they are not a lower bound on all algorithms. The assumed mixed-workload throughput is not independently measured, and overlapping CPU/I/O should be measured rather than blindly added twice.

## Verification Boundary And Next Corrections

Independent work in this review: full source read, symbolic derivations above, and 13 exact rational scalar equalities checked by BigInt cross multiplication for the pair, weighted-star, directed and dangling-normalization fixtures. All 13 passed. Three exact power-threshold checks produced the scan table. No graph runtime implementation, physical-memory test or large benchmark was run.

The source reports 36,864 stationary and 737,280 residual checks with a 1e-10 allowance (lines 194-204). Those are credited to the source, not independently rerun here. Their maximum discrepancy/shortfall figures do not constitute an interval or directed-rounding certificate.

Suggested source corrections, for its owner: make raw-versus-normalized output explicit; add the all-dangling/a=0 branches; optionally add the weighted-norm newer-state proof and exact zero-start equality; specify an error tolerance beside the batch deadline; and retain the existing "proposal, not production proof" boundary until build, RAM, disk, output and refresh admission are measured together. No source edits were made by this reader.

The primary literature linked at lines 208-212 is acknowledged as supplied prior-art context, not newly verified or fully read in this bounded mathematical review. No global novelty or measured performance claim is made here.

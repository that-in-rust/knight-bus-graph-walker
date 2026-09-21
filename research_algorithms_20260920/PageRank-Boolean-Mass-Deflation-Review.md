# Independent Boolean PageRank Mass-Deflation Review

Date: 2026-09-21. Owner scope: this document only. No implementation edits,
commits, publications, or timing benchmarks. This is an independent theorem,
finite-oracle, numerical-contract, and resource review, not an audit of the
directed-rounding implementation being developed by the lead. The separate
uniform-source baseline is outside this review's ownership.

## Findings First

**Disposition: mathematical GO for explicit-option integration; retain the
exact theorem and all-pair spectrum. This is not a code-audit, publication,
default-enablement, or resource-advantage approval.**
No counterexample was found within the stated connected, undirected,
positive-degree scope. The following are acceptance requirements and scope
clarifications, not allegations about code not reviewed here.

1. **[P1] Gap eligibility is a correctness boundary.** A complete canonical
   pair stream must prove uniqueness, completeness, constant positive height,
   exact degrees/counts, and identity with the frozen source used by the
   residual. Matching only F, n, class count, or a sampled Ritz value is not
   enough. Never accept a caller-provided spectral estimate as an upper bound.
2. **[P1] Directed variance must subtract a lower bound.** The correct direction
   is `upper(sum r_i^2/pi_i) - lower(rho^2)`, followed by upward rounding.
   Subtracting an upper bound can give a positive but invalid answer, not just
   an obviously negative one. A negative variance upper bound signals failure;
   clipping it to zero conceals an unsound enclosure.
3. **[P1] Mass is independently charged from the exact serialized scores.**
   `x=pi/2, p=pi` has V=0 and error 1/2. Rounded normalization, a solver mass
   invariant, and a residual divided by a nearly zero `1-alpha` are not
   substitutes for enclosing `abs(sum(actual_scores)-1)`.
4. **[P2] Clarify disconnected and isolate language.** Global deflation with
   lambda_bar=1 remains valid for disconnected positive-degree graphs, but
   loses the useful gap improvement. A lambda_bar<1 after removing only one
   stationary direction is false. Degree-zero vertices make this theorem's
   pi-weighted expression undefined. Teleportation does not repair that proof.
5. **[P2] Replayed output values are not a frozen-byte certificate.** The
   manuscript's final diagnostic regenerates provisional solver values in a
   dictionary. It does not read or authenticate a published binary artifact.
   This is appropriate as a rational diagnostic, not evidence of malformed-ID,
   changed-file, serialization, or publication-path coverage.
6. **[P2] Stronger directly relevant prior art exists.** Avrachenkov et al.
   explicitly separate the stationary projector in the undirected PageRank
   resolvent. Chung and Zhao combine residual propagation with reversible
   Green-function algebra. The formula is standard mathematics; possible
   differentiation remains source eligibility, checked bytes, resource
   accounting, and useful end-to-end plan selection, all still to establish.

Early actionable findings were sent before this report: the existing checker
passes; largest algebraic eigenvalue is correct; variance subtraction direction
is critical; disconnected inputs require separate stationary modes for a useful
gap; and the following two safe simplifications are available after eligibility:

```text
gamma = 1-lambda_2 = h*F / (h*(2*F-3)-1) > 1/2
lambda_bar = 1/2 is therefore an exactly representable conservative bound.
1/(1-alpha*lambda_bar) < 2 for 0 <= alpha < 1.
lower(rho^2) = 0 is always safe, although potentially less sharp.
```

These simplifications do not excuse skipped eligibility or output validation.
"Directed" in this review means directed rounding, not directed graph edges.

## Premise And Evidence Boundary

Reviewed source: `PageRank-Boolean-Mass-Deflation.md`, lines 34-136 for theorem,
family and numerical/resource claims; lines 173-281 for its embedded checker.
Source SHA-256 at inspection:
`8177d0e1735c4d0893976d286345a8ced3f15bc73b79eef7ca334dae55ff96c9`.
Frozen near-one SQLite input SHA-256:
`77ee3ce7b6837bf011fdabf8ce01bb928b1c9603fb6a13467e1e48baf9de9cb9`.
Line references describe that snapshot, not concurrent later edits.

Skills used: using-superpowers, dispatching-parallel-agents,
deep-exploration-01, verification-before-completion, and PDF inspection guidance.
Lenses: reversible spectral algebra, adversarial numerical certification,
streamed storage/accounting, and primary-literature overlap. This assigned
review is independent of the lead and baseline work; no further subagent tool
was available, and no new user-owned task was created as a substitute.
Discovery stayed in the named research documents and their evidence. No
implementation discovery or source-code audit was needed. Project functions
were executed only by the requested existing-checker replay.

Candidate approaches considered: the generic L1 residual bound; mass-deflated
weighted residual; uncentered weighted residual with mass charged separately;
and direct stationary-prior distance. The selected review target is the
mass-deflated bound, with the uncentered variant retained as a safe fallback.
The direct baseline is acknowledged but not reimplemented here.

## Exact Theorem Audit

Put `Pi=diag(pi)` and `u=sqrt(pi)`. For column-stochastic `T=A D^-1`,
`S=Pi^-1/2 T Pi^1/2=D^-1/2 A D^-1/2` is real symmetric and `S u=u`.
The zero-mass condition `1^T v=0` becomes orthogonality of `Pi^-1/2 v`
to u. Thus the relevant norm is `sum v_i^2/pi_i`, not `sum pi_i*v_i^2`.

The stationary projector is `H=pi*1^T`. It is an orthogonal projector in
that weighted inner product, although generally not in the Euclidean one.
It commutes with T. For `M=I-alpha*T`, the exact inverse separates as

```text
M^-1 r = rho/(1-alpha)*pi + M^-1(r-rho*pi)
rho/(1-alpha) = 1-s
```

On the orthogonal complement, inverse eigenvalues are
`1/(1-alpha*mu)`. They are positive and increase with mu when alpha>=0.
Consequently the largest algebraic nonprincipal eigenvalue, not the largest
absolute transition eigenvalue, controls this resolvent. The manuscript's
weighted norm bound and final Cauchy-Schwarz/triangle steps are correct.
The coefficient of the mass term is exactly one. V is the exact weighted
squared projection norm, so V>=0; finite arithmetic must preserve enclosure,
not merely mimic that algebraic identity.

### Largest Versus Absolute

- On K2, the only nonprincipal eigenvalue is -1. The centered inverse factor
  is exactly `1/(1+alpha)`, not `1/(1-alpha)`. The independent checker uses
  an antisymmetric error for which the L1 certificate is exact.
- Replacing lambda_2 by a genuine upper bound on maximum absolute eigenvalue
  is safe but can be drastically weaker. For K2 it restores the near-one
  singular-looking generic factor. Absolute eigenvalues are relevant to power
  iteration/mixing rates, which are a different question.
- The argument relies on self-adjointness, not just on knowing eigenvalues.
  For a directed nonnormal matrix, eigenvalues alone do not control the norm
  of the inverse. No directed-graph extension is licensed.
- At alpha=0 the formula is still a valid, sometimes loose, weighted-to-L1
  bound. At alpha=1 the unique personalized solution and mass identity used
  here no longer follow. Reject alpha>=1, negative alpha, and nonfinite inputs.
- An arbitrary real finite x, including signed entries, satisfies the theorem.
  A publication format may impose nonnegative scores separately. That is a
  product contract, not an omitted mathematical hypothesis.

### Disconnected And Isolate Cases

For two disjoint edges, take uniform p=pi and
`x=pi+t*(1,1,-1,-1)` with `0<t<1/4`. Its global mass error is zero while
its L1 error is 4t. The remaining zero-mass subspace contains an eigenvalue
1, and `r=-(1-alpha)*(x-pi)`. Claiming the component nonprincipal value -1
as a global gap would falsely bound the error by
`4t*(1-alpha)/(1+alpha)`. The checker detects that strict failure.

Precise extension, not required for the initial all-pair path: in each
positive-degree component c, normalize its stationary distribution pi_c to
one, let `q_c=sum(p on c)` and `m_c=sum(x on c)`, and define
`rho_c=(1-alpha)*(q_c-m_c)` and
`V_c=sum_(i in c) r_i^2/pi_c,i-rho_c^2`. Then sum
`abs(m_c-q_c)+sqrt(V_c)/(1-alpha*lambda_bar_c)` over components.
The exact PageRank component mass is q_c, not generally its share of graph
volume. Component discovery, masses and gap proofs must be charged.

An isolate has pi_i=0 under the degree rule. Replacing its outgoing column
by personalization defines a different stochastic operator, which need not
be reversible with that pi. Reject it for this theorem or prove and implement
a separate dangling decomposition. A single isolated vertex and an empty
source need explicit separate contracts, not evaluation of 0/0.

## Exact All-Pair Gap

Let `P=F*(F-1)/2`. The class graph is the line graph of K_F, with each class
replaced by h adjacent copies (a clique). The direct degree count is
`(h-1)+2*(F-2)*h=h*(2*F-3)-1`.

For the unsigned incidence matrix B, diagonal entries of `B B^T` are F-1
and off-diagonal entries are one. Its eigenvalues are `2F-2` once and
`F-2` with multiplicity F-1, so B has rank F for F>=4. The kernel of B
has dimension P-F. On class-constant vectors the adjacency is
`h*(B^T B-I)-I`; on within-class zero-sum vectors it is -I. Therefore:

| Adjacency eigenvalue | Multiplicity |
| --- | ---: |
| `d=h*(2F-3)-1` | 1 |
| `a=h*(F-3)-1` | F-1 |
| `b=-h-1` | P-F |
| `c=-1` | P*(h-1) |

The multiplicities sum to Ph=n. For h=1 the last eigenspace is absent.
For F=4,h=1, a=0, not a positive eigenvalue. For all allowed F,h,
`a>=0>b` and `a>c`, and `d-a=hF>0`. Thus the largest nonprincipal
transition eigenvalue is exactly a/d. The gap gamma=hF/d is positive;
`2*hF-d=3h+1>0` proves gamma>1/2.

The independent checker constructs adjacency by direct Boolean overlap and
checks every predicted eigenspace dimension by exact rational rank. It does
not copy the lead's PSD Schur-complement checker or use a floating eigensolver.
The full eigenspace dimensions exhaust n, so no untested eigenvalue can be
hidden between the predicted ones in these finite fixtures.

### Eligibility Contract

WHEN this specialized bound is requested, THEN a trusted immutable source
SHALL bind the following to one receipt and query:

- F is an integer >=4; every group ID is in 0..F-1. No silently discarded
  supplied unused factors, singleton signatures, empty signatures or isolates.
- Canonical signatures contain two distinct sorted IDs. A sequential check
  enumerates each `(a,b)`, `0<=a<b<F`, exactly once and verifies end-of-stream.
  Reject missing, repeated, reordered or extra records under that ordering
  contract. Raw class IDs need not be invented or renumbered to prove this.
- Every height is the same positive integer h; original-row counts and degree
  metadata agree with those heights, n=Ph and d. Preserve unique original IDs
  and the exact Boolean self/pair exclusions supplied by the trusted builder.
- Metadata, class records and original rows come from the same frozen source.
  A structural receipt is not authentication of mutable database bytes.
- Personalization remains arbitrary; uniform class heights do not mean uniform
  source weights or equal within-class PageRank. Exact normalization and alpha
  semantics are separate query checks, bound to the certificate.

An ordered stream permits O(P) record checks and constant counter state. An
unordered input needs a paid order/index/sort or another uniqueness proof.
Trusted prevalidated metadata can amortize checks, not erase preparation and
refresh costs. Recounting untrusted original rows costs O(n), not O(P).

For a generic graph, a Rayleigh quotient or largest Ritz value is normally a
lower bound on the maximum eigenvalue, the wrong direction for this use. Even
an exact residual-zero eigenpair can be a nonmaximal eigenpair. A certified
upper enclosure, exact structural derivation, or justified spectral inequality
is necessary. The K2 and C4 controls below make the direction testable.

## Actual-Output Numerical Requirements

The manuscript states the right principle at lines 118-123. A sufficient
concrete enclosure contract is below; it is not a claim that the future
implementation already follows it.

Let S enclose the exact sum of the deserialized binary64 values, and R_i
enclose each residual against the exact supplied binary64 alpha and exactly
normalized source weights. On the regular family, compute:

```text
mass_U = max(abs(S.lo-1), abs(S.hi-1)), rounded upward
Rho    = (1-alpha)*(1-S), by outward interval operations
square_U([l,u]) = max(l*l,u*u), with upward products
square_L([l,u]) = 0 when l<=0<=u; otherwise min(l*l,u*u), downward
Q_U    = upper(n * sum_i square_U(R_i))
V_U    = upper(Q_U - square_L(Rho))
den_L  = lower(1-alpha*lambda_U), strictly positive
bound_U = upper(mass_U + upper_sqrt(V_U)/den_L)
```

For nonuniform pi, each squared term must divide by a strictly positive lower
bound on pi_i. The n simplification avoids that conversion, but n and all
integer coefficients still need exact representation or outward enclosure.
Reject NaN, infinities, unsupported exponents, overflow, invalid intervals,
negative V_U, nonpositive den_L, and invalid tolerance. Do not interpret an
underflowed nearest-rounded square as a certified zero.

There is no obligation to subtract a nonzero lower bound: using zero is a
valid conservative alternative. With canonical eligibility, lambda_U=1/2
avoids rounding the exact a/d ratio. Using exact a/d is sharper and also
valid if its conversion is upward. For general small gaps, a stable expression
is `den=(1-alpha)+alpha*gamma`, with gamma a positive lower bound on the gap.

Centered accumulation `sum_i (r_i-rho*pi_i)^2/pi_i` is another valid option
when all factors are enclosed outward. Mass collected on the first existing
output pass can be available before the second residual pass; centering does
not inherently require a third output pass. It changes interval correlations
and work and must be assessed as its own schedule.

The exact formula may suffer cancellation when V is small relative to its
two terms. Correct intervals can then refuse; more precision or centering
may help. Incorrectly narrowing the interval cannot. Two exact negative
controls illustrate this:

- `r=(1/10,1/5), pi=(1/2,1/2)` gives Q=1/10, rho=3/10 and V=1/100.
  For a valid rho enclosure `[29/100,31/100]`, subtracting its upper square
  gives `39/10000`, positive but too small. A negative-value guard misses it.
- If rho lies in `[-1/10,1/10]`, its squared lower bound is zero, not 1/100.
  Endpoint-only squaring can therefore understate V even without cancellation.

### Lead's Proposed Directed Formula

The lead supplied a concrete formula during this review, before implementation
was enabled. **Mathematical verdict: GO for implementation, conditional on the
enclosures below; not approval to publish or a code audit.** Literature access
does not block this verdict. The stated source-test and certificate-test
statuses are lead reports, not tests independently replayed here.

From actual-score S, let `[m_L,m_U]` be the absolute-value image of `1-S`,
and `a_L=lower(1-alpha)>=0`. Then

```text
z_L = floor(a_L*m_L)
rho_sq_L = floor(z_L*z_L)
R_U = ceil(sum_i ceil(max(abs(r_i.lo),abs(r_i.hi))^2))
U = ceil(ceil(n*R_U)-rho_sq_L)
root_U = 0 if U=0, else context.next_plus(context.sqrt(U))
den_L = floor_exact(1-alpha*lambda_U) > 0
B_U = ceil(m_U + ceil(root_U/den_L))
```

Because both lower factors are nonnegative, `0<=z_L<=abs(rho)`, proving the
subtracted square is a lower bound. If 1-S crosses zero, m_L MUST be zero.
Endpoint magnitudes must be exact or outward-safe; Decimal `copy_abs()` avoids
an accidental default-context rounding during absolute value. Every sum and
product must use its stated context, including n conversion and accumulation.
For finite positive U, Decimal's correctly nearest-rounded square root is at
most one representable successor below the exact root, so the successor is
an upper enclosure. Use the same declared context for sqrt and next_plus;
reject nonfinite outputs, U<0, and nonpositive denominator. Exact alpha means
the supplied binary64 value, not its abbreviated decimal display.

The independent checker below additionally runs this Decimal formula from
exact independently computed residuals at 8 and 34 decimal digits. It checks
the square-root enclosure by exact rational squaring. This tests the formula
and rounding schedule only, not the lead's source module or two-pass code.

### Actual Byte Gates

WHEN publishing, THEN the verifier SHALL read all actual output records and
validate exact original IDs, class/order alignment, finite score values,
record count, complete record boundaries, and absence of trailing bytes.
Source weights must be exact binary64 rational inputs before normalization;
alpha must denote the same exact input float, not a decimal spelling such
as 0.85 substituted for its binary value.

Bind both scans, the scratch stream, and the final published bytes to one
immutable source/query/output identity. File replacement or mutation between
passes or after checking must fail or force rechecking. A converged solver,
recursive residual, output dictionary, or checksum without numerical checking
does not establish the L1 guarantee. No post-check clipping, normalization,
reserialization or byte changes without a new certificate. Rejection and
interruption must leave no public answer and close cursors/scratch handles.

Required future regressions: half-mass output; same-mass wrong scores; largest
versus absolute eigenvalue; deliberately too-small gap bound; zero-crossing
rho; positive-but-underestimated V; low precision refusal; subnormals and
extreme weights; missing/duplicate/noncanonical classes; unequal h; incorrect
d/n/F; disconnected/isolate sources; stale receipts; duplicate/unknown/missing
IDs; truncation/trailing bytes; mutation between passes; and publication or
cleanup failure. This review does not report these production tests as run.

## Resource Audit

The manuscript's qualifications at lines 125-136 are appropriate. The scalar
formula alone is not constant-memory execution. For this eligible family
`C=P=F*(F-1)/2` and `n=hP`:

| Quantity | What must remain charged |
| --- | --- |
| Eligibility | O(P) canonical class records, plus preparation/refresh and any original-row revalidation |
| Residual verification | Two full n-record output scans; O(F) factor interval objects; class/source scans and sequential C-record scratch |
| Numerical scalars | Precision-dependent coefficients, exponents, exact-weight normalization and arithmetic work |
| Storage | Retained source and indexes, temporary output, scratch, final 16n-byte `<Qd` payload and buffering |
| Whole process | SQLite cache, Python/runtime objects, allocator retention, filesystem/OS state; coordinate counts are not RSS caps |
| Plan choice | Failed eligibility, failed solves/checks, fallback attempts and default-alpha losses |

Generic Fraction growth is real, but even a fixed-denominator regular source
requires growing numerator/count bits as n or magnitude grows. A fixed number
of scalars is not fixed bytes without declared range/precision limits. A dense
n-by-n rational oracle is deliberately not a memory-constrained verifier.
Finite tests below establish no timing or physical-storage improvement.

The stationary-prior envelope is an exact corollary of the same projected
resolvent applied to `(1-alpha)*(p-pi)`, followed by a triangle inequality.
It could avoid residual arrays, but output validation and O(n) emission remain.
It needs its own numerical contract and comparison; no separate originality
or implementation credit is claimed here. Do not confuse this prior envelope
with the stronger exact-target comparison below.

### Strongest Same-Source Control

During review the lead reported that the separate control agent had obtained
an exact uniform-pair closed form for S and was adding an **exact-target
certificate**, with one output pass, F rational group sums and zero P-scratch.
This is a distinct comparator, not the stationary-prior envelope. Its new
implementation and tests were not inspected here; the status is a lead report.

If each exact target x*_i can be regenerated from those sufficient statistics,
then an upper enclosure of `sum_i abs(actual_x_i-x*_i)` directly certifies
the requested L1 error. No residual-to-error amplification is needed. This is
a mathematical consequence conditional on correctness of that closed form,
not an independent validation of the control agent's derivation.

| Same-source plan | Publication-check boundary |
| --- | --- |
| Mass-deflated residual | Two output passes; F interval arrays; P class scratch; canonical eligibility and certified gap |
| Reported exact-target control | One output pass; F rational group statistics; zero P-scratch; canonical eligibility and independently validated exact-target formula |

The comparison must keep arbitrary within-class preferences, exact binary64
input semantics, identical target error/tolerance, complete byte validation,
and frozen-source identity equal. Charge preparation and group-statistic
construction, exact-rational bit growth and arithmetic, retained data, output,
eligibility failures and all rejected attempts. Compare full solve-through-
publication rather than only the final scan. Neither one pass nor zero
P-scratch proves lower CPU or RSS without complete accounting.

This is the strongest known same-source control reported during this review.
Uniform-family practical superiority is unproved; the source-specific exact
formula could dominate this residual approach. Mass deflation may instead
serve as a generalization direction for reversible sources with useful
provable gaps but no equally cheap exact-target formula. Such generalization
requires actual eligible sources and proof, not extrapolation from this family.

## Primary Prior-Art Inspection

Search performed 2026-09-21. Publication dates below come from documents or
author metadata, not search-engine crawl dates. No absence-of-results novelty
claim is made. All summaries distinguish inspected text from search leads.

### Inspected Primary Passages

1. **Avrachenkov, Kadavankandy, Ostroumova Prokhorenkova, Raigorodskii,
   PageRank in Undirected Random Graphs, arXiv:1703.08057v1 (2017).**
   [Exact PDF](https://arxiv.org/pdf/1703.08057).
   Inspected PDF pages 2-6, especially equations (3), (6)-(9), and Theorem 1.
   Page 5 explicitly writes the symmetrized resolvent as a stationary
   projector contribution plus a sum over nonprincipal eigenvectors with
   factors `1/(1-alpha*lambda_i)`. This is the closest inspected precedent
   for the proposed zero-mode separation. Their application is an asymptotic
   approximation by a mixture of restart and degree distributions, under
   random-graph spectral hypotheses. Their absolute-gap condition concerns
   those hypotheses and an error multiplier with a lambda numerator. It does
   not invalidate the algebraic-largest-eigenvalue denominator here. This
   review's inference is that the mathematical mechanism is already standard;
   their inspected result is not a deterministic certificate of arbitrary
   serialized output from a compact Boolean source.

2. **Chung and Zhao, A sharp PageRank algorithm with applications to edge
   ranking and graph sparsification.**
   [Author PDF](https://fanchung.ucsd.edu/wp/sharp.pdf).
   Inspected PDF pages 2-6: Section 2, Lemma 1, Theorems 1-2, and Section 3
   equations (2)-(3). The paper uses lazy-walk PageRank as a shifted-Laplacian
   Green function, maintains the exact residual-source invariant
   `p=pr_(beta,s-r)`, and gives degree-normalized residual guarantees. Its
   Green-function expansion explicitly separates Laplacian eigenmodes with
   coefficients `1/(lambda_i+beta)`. These are close residual/reversible
   precedents; notation, lazy-walk parameterization and guarantee differ.
   The inspected passages do not supply a binary64-output mass-variance
   certificate or this source's eligibility proof. No publication year is
   inferred from the PDF crawl timestamp.

3. **Grolmusz, A Note on the PageRank of Undirected Graphs,
   arXiv:1205.1960v2 (2012 version identifier).**
   [Exact PDF](https://arxiv.org/pdf/1205.1960).
   Inspected PDF pages 1-3, equations (3)-(8), Corollary 1 and Theorem 2.
   The source derives `PageRank-degree_prior` by applying the resolvent to
   `personalization-degree_prior`, and supplies upper/lower L1 comparisons.
   It proves equality for degree-distributed personalization, not for arbitrary
   personalization. The inspected theorem does not use a nonprincipal gap or
   certify an arbitrary provisional x. This is direct but less specific
   precedent than item 1. The retrieved PDF also displays a later typesetting
   date; the arXiv version identifier is the date basis used here.

4. **Sun, Huang, Carpentieri and Jing, Flexible and deflated variants of the
   block shifted GMRES method, JCAM 345 (2019), 168-183.**
   [Repository version-of-record PDF](https://pure.rug.nl/ws/files/102596099/1_s2.0_S0377042718303303_main.pdf),
   [DOI](https://doi.org/10.1016/j.cam.2018.05.053).
   Inspected repository PDF pages 2-6, especially printed pages 171-172,
   Section 2 and equations (12)-(14). The paper distinguishes removal of
   near-dependent block residual/right-hand-side directions using SVD from
   spectral deflation of small system eigenvalues. It includes PageRank as an
   application. This is genuine deflation prior art, but not the same as
   removing the known stationary direction and charging its mass error.
   Citing the word "deflation" alone would overstate the overlap. Its solver
   acceleration is not a certified upper gap bound for our output validator.

### Limited Inspection And Search-Only Leads

- **Wills and Ipsen, Ordinal Ranking for Google's PageRank (2009).**
  [Author PDF](https://ipsen.math.ncsu.edu/ps/simax69812.pdf).
  Search-index excerpts identify Theorem 4.7 forward-looking residual bounds,
  Section 5 finite-precision analysis and Theorem 5.1. The PDF endpoint returned
  document metadata, but targeted text and screenshot retrievals failed.
  Those passages were not independently read in the PDF here. Strong
  residual/roundoff lead, not evidence of the exact mass-deflation formula.
- **Nathan, Sanders, Henson and Bader, Numerically approximating centrality
  for graph ranking guarantees (2018).**
  [Inspected author abstract](https://davidbader.net/publication/2018-nshb/),
  [PDF lead](https://davidbader.net/publication/2018-nshb/2018-nshb.pdf).
  The abstract describes residual-based pairwise-ranking guarantees and
  probabilistic norms. Targeted PDF-body retrieval failed after metadata was
  returned. Abstract-only inspection; no theorem comparison claimed.
- **Kamvar and Haveliwala, The Condition Number of the PageRank Problem.**
  [Primary-paper copy](https://didawiki.di.unipi.it/lib/exe/fetch.php/mcl/kamvar03condition.pdf).
  Search excerpts expose inverse L1-norm and condition-number lemmas. Not
  opened/inspected in this review; no reliance on its broader accuracy claims.
- **Aldous and Fill, Reversible Markov Chains and Random Walks on Graphs.**
  [Book](https://www.stat.berkeley.edu/users/aldous/RWG/book.pdf).
  Search-only/background lead in this review. The manuscript records limited
  retrieval of its table of contents. Neither is represented as full-section
  or full-book inspection.

Search phrases included PageRank residual error bounds/stationary distribution,
rank-one and mass deflation, reversible spectral gap/residual, Ipsen/Wills
finite precision, and deflated PageRank linear systems. Irrelevant legal/SEO
uses of "PageRank deflation" and nonprimary summaries were excluded. The
inspected papers establish substantial known mathematical overlap, not an
exhaustive closest-paper ranking or a literature novelty clearance.

## Existing Checker Replay

Executed from the repository root with Python 3.11 and bytecode writes disabled:

```sh
awk '/^```python$/{block=1;next} /^```$/{if(block)exit} block' research_algorithms_20260920/PageRank-Boolean-Mass-Deflation.md | /Users/amuldotexe/.local/bin/python3.11 -B
```

Exit status 0. Output:

```text
Exact PSD source checks: 7 error-bound cases: 168
Mass-omission negative control: centered variance zero, required error bound 1/2
factor-cg actual 6.289259834586463e-17 generic 0.45072079401236576 deflated 1.4097849144397902e-16
class-cg actual 8.326672684688674e-16 generic 0.4995932313699968 deflated 9.326207751613067e-16
No output published; exact-rational diagnostic only.
```

These reproduce the manuscript's numbers exactly at displayed precision.
They are not 168 independent real sources. The oracle is the lead's existing
one, so replay is not independent mathematical implementation. The next
checker closes that narrower independence gap.

## Independent Finite Checker

The sole Python block below imports only the standard library. It uses its
own dense rational solve; validates the solution by direct substitution;
checks eigenvalue multiplicities by exact rank; and compares squared bounds
without computing square roots. Constructed candidates are round-tripped
through binary64 bytes in memory. Its interval tests independently verify
enclosure algebra starting from exact residuals, not the lead's Decimal
implementation or its streaming residual construction. It writes no files.

```python
from fractions import Fraction as Q
from itertools import combinations
from decimal import Context, Decimal, ROUND_CEILING, ROUND_FLOOR
import math
import struct


def compute_rational_matrix_rank(matrix):
    work = [[Q(value) for value in row] for row in matrix]
    rank = 0
    for column in range(len(work[0])):
        pivot = next((row for row in range(rank, len(work))
                      if work[row][column]), None)
        if pivot is None:
            continue
        work[rank], work[pivot] = work[pivot], work[rank]
        scale = work[rank][column]
        work[rank] = [value / scale for value in work[rank]]
        for row in range(rank + 1, len(work)):
            scale = work[row][column]
            if scale:
                work[row] = [value - scale * base
                             for value, base in zip(work[row], work[rank])]
        rank += 1
        if rank == len(work):
            break
    return rank


def solve_rational_linear_system(matrix, right):
    n = len(right)
    work = [[Q(value) for value in row] + [Q(value)]
            for row, value in zip(matrix, right)]
    for column in range(n):
        pivot = next(row for row in range(column, n) if work[row][column])
        work[column], work[pivot] = work[pivot], work[column]
        scale = work[column][column]
        work[column] = [value / scale for value in work[column]]
        for row in range(n):
            if row != column and work[row][column]:
                scale = work[row][column]
                work[row] = [value - scale * base
                             for value, base in zip(work[row], work[column])]
    answer = [row[-1] for row in work]
    assert all(sum(value * x for value, x in zip(row, answer)) == target
               for row, target in zip(matrix, right))
    return answer


def construct_signature_overlap_matrix(signatures):
    return [[int(i != j and bool(set(left).intersection(right)))
             for j, right in enumerate(signatures)]
            for i, left in enumerate(signatures)]


def construct_edge_adjacency_matrix(n, edges):
    matrix = [[0] * n for _ in range(n)]
    for left, right in edges:
        matrix[left][right] = matrix[right][left] = 1
    return matrix


def construct_reversible_transition_matrix(adjacency):
    degrees = list(map(sum, adjacency))
    if not degrees or min(degrees) <= 0:
        raise ValueError('positive degrees required')
    n, volume = len(degrees), sum(degrees)
    transition = [[Q(adjacency[i][j], degrees[j]) for j in range(n)]
                  for i in range(n)]
    stationary = [Q(degree, volume) for degree in degrees]
    assert all(sum(transition[i][j] for i in range(n)) == 1 for j in range(n))
    assert all(transition[i][j] * stationary[j] ==
               transition[j][i] * stationary[i] for i in range(n) for j in range(n))
    return transition, stationary


def verify_exact_eigenspace_dimensions(matrix, spectrum):
    n = len(matrix)
    assert sum(spectrum.values()) == n
    for eigenvalue, multiplicity in spectrum.items():
        shifted = [[value - eigenvalue * int(i == j)
                    for j, value in enumerate(row)] for i, row in enumerate(matrix)]
        assert n - compute_rational_matrix_rank(shifted) == multiplicity


def compute_vector_transition_product(transition, vector):
    return [sum(value * entry for value, entry in zip(row, vector))
            for row in transition]


def evaluate_deflated_error_squares(transition, stationary, p, alpha, x, truth, bound):
    incoming = compute_vector_transition_product(transition, x)
    residual = [(1-alpha)*p[i] + alpha*incoming[i] - x[i] for i in range(len(x))]
    mass, rho = sum(x)-1, sum(residual)
    assert rho == -(1-alpha)*mass
    variance = sum(r*r/pi for r, pi in zip(residual, stationary)) - rho*rho
    centered = [r-rho*pi for r, pi in zip(residual, stationary)]
    assert variance == sum(r*r/pi for r, pi in zip(centered, stationary)) >= 0
    error = sum(abs(value-exact) for value, exact in zip(x, truth))
    denominator = 1-alpha*bound
    assert denominator > 0
    lhs = max(Q(0), error-abs(mass))**2 * denominator**2
    return lhs <= variance, residual, variance, error


def enclose_rational_value_dyadic(value, bits):
    scale = 1 << bits
    number = value * scale
    return (Q(number.numerator // number.denominator, scale),
            Q(-((-number.numerator) // number.denominator), scale))


def evaluate_interval_square_extrema(interval):
    low, high = interval
    assert low <= high
    lower = Q(0) if low <= 0 <= high else min(low*low, high*high)
    return lower, max(low*low, high*high)


def verify_directed_enclosure_algebra(residual, stationary, x, alpha, bound, variance, error, bits):
    intervals = [enclose_rational_value_dyadic(value, bits) for value in residual]
    mass = enclose_rational_value_dyadic(sum(x), bits)
    rho = (enclose_rational_value_dyadic((1-alpha)*(1-mass[1]), bits)[0],
           enclose_rational_value_dyadic((1-alpha)*(1-mass[0]), bits)[1])
    upper = Q(0)
    for interval, pi in zip(intervals, stationary):
        upper = enclose_rational_value_dyadic(
            upper + evaluate_interval_square_extrema(interval)[1]/pi, bits)[1]
    upper = enclose_rational_value_dyadic(
        upper - evaluate_interval_square_extrema(rho)[0], bits)[1]
    assert upper >= variance >= 0
    lambda_upper = enclose_rational_value_dyadic(bound, bits)[1]
    denominator = enclose_rational_value_dyadic(1-alpha*lambda_upper, bits)[0]
    assert denominator > 0
    mass_upper = max(abs(mass[0]-1), abs(mass[1]-1))
    assert max(Q(0), error-mass_upper)**2 * denominator**2 <= upper


def verify_proposed_decimal_formula(residual, x, alpha, bound, variance, error, precision):
    down = Context(prec=precision, rounding=ROUND_FLOOR)
    up = Context(prec=precision, rounding=ROUND_CEILING)
    zero, one = Decimal(0), Decimal(1)
    mass_low = mass_high = zero
    residual_high = zero
    for value in x:
        mass_low = down.add(mass_low, down.divide(Decimal(value.numerator), Decimal(value.denominator)))
        mass_high = up.add(mass_high, up.divide(Decimal(value.numerator), Decimal(value.denominator)))
    low, high = down.subtract(one, mass_high), up.subtract(one, mass_low)
    mass_abs_low = zero if low <= zero <= high else min(low.copy_abs(), high.copy_abs())
    mass_abs_high = max(low.copy_abs(), high.copy_abs())
    reset = 1-alpha
    reset_low = down.divide(Decimal(reset.numerator), Decimal(reset.denominator))
    rho_low = down.multiply(reset_low, mass_abs_low)
    rho_square_low = down.multiply(rho_low, rho_low)
    for value in residual:
        rlow = down.divide(Decimal(value.numerator), Decimal(value.denominator))
        rhigh = up.divide(Decimal(value.numerator), Decimal(value.denominator))
        magnitude = max(rlow.copy_abs(), rhigh.copy_abs())
        residual_high = up.add(residual_high, up.multiply(magnitude, magnitude))
    upper = up.subtract(up.multiply(Decimal(len(x)), residual_high), rho_square_low)
    assert upper.is_finite() and Q(upper) >= variance >= 0
    root = zero if upper == zero else up.next_plus(up.sqrt(upper))
    assert root.is_finite() and Q(root)**2 >= Q(upper)
    lambda_upper = up.divide(Decimal(bound.numerator), Decimal(bound.denominator))
    denominator = 1-alpha*Q(lambda_upper)
    denominator_low = down.divide(Decimal(denominator.numerator), Decimal(denominator.denominator))
    assert denominator_low > zero
    certified = up.add(mass_abs_high, up.divide(root, denominator_low))
    assert certified.is_finite() and Q(certified) >= error


def certify_canonical_pair_records(factors, height, vertices, records):
    if factors < 4 or height < 1 or vertices != height*factors*(factors-1)//2:
        return False
    stream = iter(records)
    degree = height*(2*factors-3)-1
    for left in range(factors-1):
        for right in range(left+1, factors):
            if next(stream, None) != ((left, right), height, degree):
                return False
    return next(stream, None) is None


fixtures = []
family_parameters = ((4,1), (4,2), (5,1), (5,2), (6,1), (6,2), (8,2))
for factors, height in family_parameters:
    pairs = list(combinations(range(factors), 2))
    adjacency = construct_signature_overlap_matrix([pair for pair in pairs for _ in range(height)])
    classes, n = len(pairs), len(adjacency)
    degree, numerator = height*(2*factors-3)-1, height*(factors-3)-1
    assert all(sum(row) == degree for row in adjacency)
    spectrum = {degree: 1, numerator: factors-1, -height-1: classes-factors,
                -1: classes*(height-1)}
    verify_exact_eigenspace_dimensions(adjacency, spectrum)
    transition, stationary = construct_reversible_transition_matrix(adjacency)
    spectral = Q(numerator, degree)
    assert Q(0) <= spectral < Q(1,2)
    assert 1-spectral == Q(height*factors, degree)
    fixtures.append((transition, stationary, spectral, True))

small_graphs = (
    (2, [(0,1)], {Q(1):1, Q(-1):1}),
    (3, [(0,1),(1,2),(2,0)], {Q(1):1, Q(-1,2):2}),
    (3, [(0,1),(1,2)], {Q(1):1, Q(0):1, Q(-1):1}),
    (4, [(0,1),(1,2),(2,3)], {Q(1):1, Q(1,2):1, Q(-1,2):1, Q(-1):1}),
    (4, [(0,1),(0,2),(0,3)], {Q(1):1, Q(0):2, Q(-1):1}),
    (4, [(0,1),(1,2),(2,3),(3,0)], {Q(1):1, Q(0):2, Q(-1):1}),
)
for n, edges, spectrum in small_graphs:
    transition, stationary = construct_reversible_transition_matrix(construct_edge_adjacency_matrix(n, edges))
    verify_exact_eigenspace_dimensions(transition, spectrum)
    fixtures.append((transition, stationary, max(mu for mu in spectrum if mu != 1), False))

candidate_checks = interval_checks = fallback_checks = decimal_checks = 0
alphas = (Q(0), Q(1,2), Q(0.85), Q(math.nextafter(1.0, 0.0)))
for transition, stationary, spectral, all_pairs in fixtures:
    n = len(stationary)
    weights = [Q((i*7+3) % 11) for i in range(n)]
    preferences = ([Q(1,n)]*n, [Q(int(i==0)) for i in range(n)],
                   [value/sum(weights) for value in weights])
    for p in preferences:
        for alpha in alphas:
            matrix = [[Q(int(i==j))-alpha*transition[i][j] for j in range(n)] for i in range(n)]
            truth = solve_rational_linear_system(matrix, [(1-alpha)*value for value in p])
            assert sum(truth) == 1 and min(truth) >= 0
            rounded = [Q(float(value)) for value in truth]
            candidates = [rounded, [value/2 for value in rounded],
                          [value+Q(int(i==0),64) for i,value in enumerate(rounded)],
                          [value+Q(int(i==0)-int(i==1),64) for i,value in enumerate(rounded)],
                          [-value for value in rounded], [Q(0)]*n]
            for candidate in candidates:
                raw = b''.join(struct.pack('<d', float(value)) for value in candidate)
                x = [Q(value[0]) for value in struct.iter_unpack('<d', raw)]
                passed, residual, variance, error = evaluate_deflated_error_squares(
                    transition, stationary, p, alpha, x, truth, spectral)
                assert passed
                candidate_checks += 1
                for bits in (12, 80):
                    verify_directed_enclosure_algebra(residual, stationary, x, alpha,
                                                     spectral, variance, error, bits)
                    interval_checks += 1
                if all_pairs:
                    assert evaluate_deflated_error_squares(
                        transition, stationary, p, alpha, x, truth, Q(1,2))[0]
                    fallback_checks += 1
                    for precision in (8,34):
                        verify_proposed_decimal_formula(residual, x, alpha, spectral,
                                                        variance, error, precision)
                        decimal_checks += 1

# Exact counterexamples must fail the deliberately weakened assertions.
alpha, t = Q(0.85), Q(1,16)
transition, pi = construct_reversible_transition_matrix([[0,1],[1,0]])
passed, residual, variance, error = evaluate_deflated_error_squares(
    transition, pi, pi, alpha, [Q(1,4)]*2, pi, Q(-1))
assert passed and variance == 0 and error == Q(1,2)
x = [Q(1,2)+t, Q(1,2)-t]
passed, residual, variance, error = evaluate_deflated_error_squares(transition, pi, pi, alpha, x, pi, Q(-1))
assert passed and error**2*(1+alpha)**2 == variance
assert variance/(1-alpha)**2 > error**2
largest = Q(float.fromhex('0x1.fffffffffffffp+1023'))
smallest = Q(float.fromhex('0x0.0000000000001p-1022'))
for x in ([smallest,Q(0)], [largest,largest], [largest,-largest], [Q(1,2),Q(1,2)]):
    passed, residual, variance, error = evaluate_deflated_error_squares(
        transition, pi, pi, alpha, x, pi, Q(-1))
    assert passed
    for precision in (8,34):
        verify_proposed_decimal_formula(residual, x, alpha, Q(-1), variance, error, precision)
        decimal_checks += 1

disconnected = construct_edge_adjacency_matrix(4, [(0,1),(2,3)])
transition, pi = construct_reversible_transition_matrix(disconnected)
x = [pi[i]+t*(1 if i<2 else -1) for i in range(4)]
assert not evaluate_deflated_error_squares(transition, pi, pi, alpha, x, pi, Q(-1))[0]
assert evaluate_deflated_error_squares(transition, pi, pi, alpha, x, pi, Q(1))[0]
cycle = construct_edge_adjacency_matrix(4, [(0,1),(1,2),(2,3),(3,0)])
transition, pi = construct_reversible_transition_matrix(cycle)
x = [pi[i]+t*(int(i==0)-int(i==2)) for i in range(4)]
assert not evaluate_deflated_error_squares(transition, pi, pi, alpha, x, pi, Q(-1))[0]
assert evaluate_deflated_error_squares(transition, pi, pi, alpha, x, pi, Q(0))[0]
assert compute_vector_transition_product(transition, [1,-1,1,-1]) == [-1,1,-1,1]

try:
    construct_reversible_transition_matrix(construct_edge_adjacency_matrix(3, [(0,1)]))
except ValueError:
    pass
else:
    raise AssertionError('isolate must be rejected')

q, rho = Q(1,10), Q(3,10)
exact_variance = q-rho*rho
bad_upper = q-evaluate_interval_square_extrema((Q(29,100),Q(31,100)))[1]
assert 0 < bad_upper < exact_variance
assert evaluate_interval_square_extrema((Q(-1,10),Q(1,10)))[0] == 0
assert Q(1,25)-Q(1,100) < Q(1,25)

records = [(pair, 2, 9) for pair in combinations(range(4),2)]
assert certify_canonical_pair_records(4,2,12,records)
malformed = [records[:-1], records+[records[-1]], records[::-1],
             [records[0]]+records[:-1],
             [((0,0),2,9)]+records[1:], [((0,4),2,9)]+records[1:],
             [((0,1),1,9)]+records[1:], [((0,1),2,8)]+records[1:]]
assert all(not certify_canonical_pair_records(4,2,12,value) for value in malformed)
assert not certify_canonical_pair_records(4,2,11,records)
assert not certify_canonical_pair_records(3,2,12,records)

print('Independent exact spectra:', len(fixtures), '(7 all-pair; 6 small reversible)')
print('Independent binary64 candidate inequalities:', candidate_checks)
print('Independent dyadic interval inequalities:', interval_checks)
print('All-pair lambda_bar=1/2 fallback inequalities:', fallback_checks)
print('Proposed Decimal formula inequalities (8/34 digits, including extremes):', decimal_checks)
print('Negative controls: mass, absolute-gap loss, disconnected, false Ritz gap, isolate, variance direction, zero crossing: PASS')
print('Canonical stream controls: 1 eligible, 10 ineligible: PASS')
print('No project imports, files written, publication, or timing benchmarks.')
```

Reproduction from repository root:

```sh
awk '/^```python$/{block=1;next} /^```$/{if(block)exit} block' research_algorithms_20260920/PageRank-Boolean-Mass-Deflation-Review.md | /Users/amuldotexe/.local/bin/python3.11 -B
```

## Independent Execution Receipt

Fresh execution of the complete embedded checker exited 0:

```text
Independent exact spectra: 13 (7 all-pair; 6 small reversible)
Independent binary64 candidate inequalities: 936
Independent dyadic interval inequalities: 1872
All-pair lambda_bar=1/2 fallback inequalities: 504
Proposed Decimal formula inequalities (8/34 digits, including extremes): 1016
Negative controls: mass, absolute-gap loss, disconnected, false Ritz gap, isolate, variance direction, zero crossing: PASS
Canonical stream controls: 1 eligible, 10 ineligible: PASS
No project imports, files written, publication, or timing benchmarks.
```

The 1,016 Decimal checks consist of 504 all-pair candidates at two precisions
plus four K2 extreme/zero controls at two precisions. The 936 general candidate
checks include signed and mass-defective vectors, and both regular and irregular
reversible graphs. The canonical checks validate the finite specification
oracle, not any new source module. Tests are finite evidence supplementing the
proof, not a substitute for it or for production regressions.

## Terminal Correctness Handoff

**This assigned theorem/resource/prior-art review and its independent checker
are terminal.** No in-scope correctness blocker remains for integrating the
directed formula behind the explicit certificate option under the stated
conditions. The lead's report of 12 green standalone tests is not independent
code-audit evidence; new implementation inspection was explicitly excluded.
No default, published result, benchmark reinterpretation or practical win is
approved by this report.

Closed here: core proof, exact all-pair gap and multiplicities, largest versus
absolute eigenvalue, mass omission, disconnected/isolate boundaries,
directional variance and square root, finite oracle replay, independent
checker, numerical requirements, resource comparison boundary, and scoped
primary-literature search with inspected passages identified.

Remaining outside this review: production actual-byte integration regressions,
the separate exact-target control's proof/code, lifecycle performance and
physical memory enforcement, useful nonfixture source prevalence, and a
scientific differentiation claim. Search-only literature leads remain marked
as such; their retrieval does not reopen the completed mathematical verdict.
Only this owned Markdown was authored. No other-file/code edits or commits.

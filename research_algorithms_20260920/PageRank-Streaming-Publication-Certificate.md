# Streaming Publication With A Scalar Error Ledger

Date: 2026-09-21. A01 research derivation with a subsequent separate implementation. A [completed independent mathematical review](PageRank-Streaming-Certificate-Review.md) supports the conditional bound and identifies a stronger published summation control, now included. Implementation, tests and measured scope are tracked in the [experiment record](PageRank-Streaming-Certificate-Experiment.md). **Scientific novelty and whole-job physical RAM benefit are not established.** This note follows the completed [mass-correction experiment](PageRank-Mass-Correction-Experiment.md). It does not change that experiment's frozen code, results or guarantees.

## Premise Check

The latest experiment has an important lifecycle mismatch: the projected iteration uses only two F-sized binary64 buffers, but the final certifier retains two F-sized Decimal endpoint arrays and performs two more full source/output scans. The current public graph is too small for selected-buffer savings to establish lower process RAM. Certification is nevertheless a concrete paid phase: it consumes about 20% of projected session time, 26% of factor-CG time and 45% of reused-dense time in the median of each method's paired-run session fractions. Those percentages describe this Python experiment, not a general implementation lower bound.

The next question is narrow: **can the act of generating a complete score file also produce its rigorous error bound, using scalar high-precision error ledgers instead of high-precision arrays?** If so, strong existing solvers must receive the same publication path. Improving certification is not evidence that the mass-corrected solver itself is superior.

## Expert Lenses

- Numerical analysis: bound the error of actual binary64 output, not only an exact-arithmetic state or an uncertified residual screen.
- Storage execution: distinguish one source pass from output verification, hash passes, buffered row memory and construction costs.
- Skeptical comparison: an ordinary CG solve may benefit at least as much; interval arithmetic and positive-sum bounds are established techniques.
- Product accounting: a predictable refusal or fallback is useful, but an arbitrary-graph physical RAM guarantee requires an admitted builder and a complete allocator/cache/output budget.

## Candidate Approaches

| Option | Mechanism | Main tradeoff | Status |
| --- | --- | --- | --- |
| Current reference | Read the completed output, gather lower/upper factor intervals, evaluate original residual in a second scan | Strong direct check of actual file; two F-sized high-precision arrays and extra passes | Implemented and used in all frozen results |
| Proposed scalar ledger | Enclose ideal scores as they are written; scatter rounded row values into one f64 factor array; charge all rounding through weighted scalar bounds | Removes high-precision factor arrays; positive-sum admission and conservative summation floor; more scalar interval work during publication | Implemented separately with gamma and stronger published ufp bounds |
| External contribution reduction | Emit `(factor, rounded row value)` records, externally sort, reduce one factor at a time with exact/directed arithmetic | Avoids an F-sized scatter array, but adds membership-sized scratch, sorting passes and preparation; h access still needs a separately admitted schedule | Alternative design, no new sorting primitive or cost claim |
| Blocked direct certificate | Process a bounded factor block per source/output replay | Preserves the original certificate logic with less live interval state, but needs multiple scans and a bounded output/row schedule | Required low-RAM comparator, not implemented here |

The chosen first test is the scalar ledger because it targets an observed phase with a precise proof obligation. It does not require changing the graph algorithm or discovering new factors. The other options matter when summation bounds are too conservative or F-sized binary64 state itself does not fit.

## 1. Source And Answer Contract

Use the native binary-incidence contract already validated by the existing importer. After pruning singleton factors, B is a binary vertex-by-factor incidence matrix, with retained factor cardinalities s_f>=2. Repeated groups are separate factors; repeated membership within one factor is forbidden. Preserve isolated vertices in the vertex universe.

```text
s = B^T 1                         exact integer factor cardinalities
q = B 1                           exact integer memberships per vertex
d = B s - q                       exact integer weighted degrees
A = B B^T - diag(q)                loop-free weighted clique projection
K = D + a Q                       active vertices only
```

Fix rational damping 0<a<1 and a nonnegative normalized personalization p. For the existing contract a=17/20; p is uniform or a point mass. Let pZ be the EXACT personalization mass on isolated vertices. Do not compute it by repeatedly adding rounded uniform probabilities.

```text
bEffective = (1-a) p / (1-a*pZ)
```

On active vertices the stationary solution satisfies `x = bEffective + a A D^-1 x`. On isolates its exact score is bEffective. This isolates-only elimination is specific to the symmetric source contract. It is not a shortcut for arbitrary directed dangling vertices.

Treat the supplied finite binary64 h as an exact dyadic vector. It may come from factor CG, reduced power, projection or a direct solve. Define

```text
y_i(h) = (bEffective_i + a sum_{f in row_i} h_f) / (d_i+a*q_i)
X_i(h) = d_i*y_i(h)                active vertices
X_i(h) = bEffective_i              isolated vertices
T(h)   = B^T y(h)
r(h)   = T(h)-h
```

The known reconstruction identity gives an original-equation residual `a B r(h)` on active vertices and zero on isolates. The full PageRank operator, including the specified dangling rule, is column-stochastic. Its L1 resolvent bound therefore gives

```text
||x* - X(h)||_1 <= a/(1-a) sum_f s_f |r_f(h)|.             (1)
```

This is the existing residual argument, not a new PageRank theorem. The proposed change is how to safely obtain a bound for (1) and the emitted bytes without storing interval vectors.

## 2. Publication Pass

Keep h immutable and clear one F-sized binary64 scatter vector z. For each active source row:

1. Compute an outward-rounded scalar interval `[L_i,U_i]` enclosing exact y_i(h). Decode h from its actual binary64 bits; enclose every addition, rational coefficient and division. The positive denominator d_i+a*q_i is exact rational before enclosure.
2. Choose the binary64 value `yhat_i` actually used for scatter. Obtain an upward scalar `e_i >= max(|yhat_i-L_i|, |yhat_i-U_i|)`. Do not assume conversion to binary64 is itself exact; this distance explicitly accounts for it.
3. Require yhat_i>=0 and finite. For a conservative first implementation, require every nonzero yhat_i to be normal binary64. Reject or use the reference certifier otherwise. This is an admission rule for the proposed summation proof, not a reason to clip negative values.
4. For each factor in the row, perform the explicitly specified sequential binary64 addition `z_f = RN(z_f+yhat_i)`. Reject any nonfinite result. No parallel atomics, hidden reduction tree, mixed precision or reassociation is admitted without its own bound.
5. Emit the actual binary64 score xhat_i. For example it may be RN(d_i*yhat_i), but the proof only requires that the SAME emitted bits are enclosed by a scalar publication-error bound `eta_i >= max(|xhat_i-d_i*L_i|, |xhat_i-d_i*U_i|)`. Compute endpoint multiplication and distances outward; do not silently round d_i to a different integer degree.
6. Accumulate, with upward rounding, `Erow += (d_i+q_i)*e_i` and `Epub += eta_i`.

For isolates, emit an approximation to bEffective and add its outward error to Epub. They have no factor scatter. F=0 bypasses the factor stage; Epub alone bounds the full output error.

All error ledgers are scalars. The current row decoder still retains a whole row, so peak state includes O(max_i q_i) IDs. Making row memory independent of maximum degree may require a bounded scratch/replay layout: the row sum must be known before its memberships can receive yhat_i. Calling this a single source pass does not solve that importer/decoder problem for free.

## 3. Collapse Row Error Without An Error Vector

Let `Q_f = sum_{i in f} yhat_i` be the exact sum of the rounded row values. Since each |yhat_i-y_i| is at most e_i,

```text
sum_f s_f |Q_f - T_f(h)|
  <= sum_f s_f sum_{i in f} e_i
   = sum_i e_i sum_{f in row_i} s_f
   = sum_i (d_i+q_i) e_i
  <= Erow.                                                   (2)
```

The last identity uses exact `B s=d+q`. This is the graph-specific scheduling opportunity: the source row already contains d_i and its membership count, so the weighted error charge can be accumulated immediately without an F-sized high-precision error array or random cardinality lookups.

It is an elementary exchange of nonnegative sums. Do not promote that exchange alone as a scientific novelty. The question is whether the complete encoded certificate produces a useful, defensible RAM/work frontier.

## 4. Bound Scatter Rounding From The Final Factor Values

For sequential round-to-nearest binary64 addition of nonnegative admitted values, use unit roundoff `u=2^-53`. A conservative operation-count bound is m_f=s_f, including the exact initial addition to zero. Define

```text
gamma_f = m_f*u / (1-m_f*u).
```

Admit only gamma_f<1, equivalently m_f*u<1/2. Under the stated normal/nonoverflowing arithmetic contract, the standard recursive-summation bound gives

```text
|z_f-Q_f| <= gamma_f * Q_f.
```

Positivity then permits an a-posteriori bound using the available rounded sum:

```text
Q_f <= z_f/(1-gamma_f)
|z_f-Q_f| <= gamma_f/(1-gamma_f) * z_f.                      (3)
```

Now make one sequential pass over exact factor cardinalities. Read z_f and h_f as their exact binary64 values. With scalar directed arithmetic, accumulate

```text
Rhat     >= sum_f s_f |z_f-h_f|
Escatter >= sum_f s_f * gamma_f/(1-gamma_f) * z_f.
```

Each gamma and denominator must be enclosed in the safe direction. A usual floating evaluation of the formula followed by an unproved safety multiplier is not enough. Using each s_f is tighter than replacing every gamma_f with gamma_max and costs no additional factor-sized arrays. The size records and source identity must be bound to the same validated graph snapshot.

The arithmetic foundation is established: Higham, [The Accuracy of Floating Point Summation (1993)](https://nhigham.com/wp-content/uploads/2023/10/high93s.pdf), Section 1 model (1.2), Section 2 product bound and equation (2.6). The extracted full text explicitly assumes no underflow in its relative-error model. This proposal uses a conservative positive-normal admission rule instead of silently extending that model to every machine mode. No claim of a new gamma bound is made.

### Stronger Published Same-State Control

Independent review identified Rump, [Error Estimation of Floating-Point Summation and Dot Product (2012)](https://www.tuhh.de/ti3/paper/rump/altRu11.pdf), Theorem 3.5. For the same sequential nonnegative scatter, replace the weighted contribution in Escatter by

```text
s_f * (s_f-1) * 2^-53 * ufp(z_f),
ufp(z) = largest power of two <= z, with ufp(0)=0.
```

The lead inspected the primary theorem and its same-order condition. Positivity makes the signed sum and absolute-sum accumulator identical here. This is tighter on the gamma-admitted domain and needs no extra factor array. The implementation extracts the exponent from the binary64 representation and evaluates the weighted bound with directed Decimal arithmetic. The theorem has no summand-count restriction; this does not excuse finite-range checks in an implementation. The conservative positive-normal policy remains unchanged.

Thus the earlier gamma cutoff is a restriction of one bound, not an inherent barrier to low-memory certification. The gamma variant is retained as an explicit control. A two-vertex exact-answer test demonstrates the difference: the gamma publisher refuses tolerance 2e-15 while the stronger control accepts exactly the same `(0.5,0.5)` bytes. This improved admission is credited to the published bound, not to a new theorem in this project.

## 5. Certificate For Published Bytes

Combining (2), (3) and the triangle inequality yields

```text
sum_f s_f |T_f(h)-h_f| <= Rhat + Escatter + Erow.
```

Combining this with (1) and the directly accumulated output error gives the proposed certificate:

```text
Efinal = upward( a/(1-a) * (Rhat+Escatter+Erow) + Epub )

||x* - xhat||_1 <= Efinal.                                  (4)
```

This bounds the actual output vector, including its rounding and isolated scores. It does not claim bitwise equality with another solver or with Neo4j/GDS, and it does not require h to be the exact solution. If Efinal exceeds the requested tolerance, refuse publication or invoke a separately admitted fallback.

Equation (4) is a derivation for the declared arithmetic/source contract. The separate implementation explicitly computes these ledgers; its tests and review are different evidence from the proof. The solver's uncertified floating residual screen is NOT substituted for Rhat, Escatter or Erow.

## 6. Transaction And Provenance Boundary

Generate into a private temporary score file. Update a streaming content hash from the exact byte buffer being written, require successful complete writes, and bind the result to source hash, cardinality metadata hash, a, personalization, h identity and arithmetic policy. Publish by atomic replacement only if the final bound passes. A separate readback hash or stronger durable storage validation, if required, adds an output read and must be charged.

This protects the mathematical correspondence between emitted numbers and the certificate; it is not a proof against a faulty disk, concurrent source replacement, crashes or a malicious builder. Source validation, immutable snapshot identity and durable publication are separate contracts. The current direct certifier reads actual file bytes and can detect output-generation mistakes that this fused path could share. Keep it as an independent development oracle and optional production verification mode.

The implemented research API also requires exclusive ownership of h during publication. Hashing mutable paths or state once is not protection against concurrent change-and-restore races. Its manifest builder validates exact factor cardinalities and `d=B*s-q` before issuing a trusted receipt; the query's hashes preserve identity, not an independent reproof of those facts. It decodes each packed score before charging Epub, then writes and hashes that same buffer. Only the score file is atomically replaced; the certificate is returned to the caller, not persisted in a crash-atomic two-file generation. No durable score-plus-certificate transaction is claimed.

If the proposed certificate fails after output generation, the reference fallback's interval arrays must fit an explicitly reserved budget, or the system must refuse. An emergency allocation beyond the advertised RAM cap is not an acceptable fallback. If a lower-RAM blocked fallback is implemented instead, account for its extra passes and reserve scratch first.

## 7. Predicted Resource Frontier

Let Z be memberships, N vertices and F factors. The following are architectural counts, not measured speed or RSS estimates.

| Dimension | Current reconstruct-then-certify route | Proposed fused route |
| --- | --- | --- |
| Source passes after factor h is ready | One reconstruction, then two certificate passes | One publication/scatter pass, plus a factor-cardinality pass |
| Output reads for numerical proof | Two | None under the same-write-buffer contract; optional readback is separately paid |
| Main certificate state | Two F-sized Decimal endpoint arrays; solver arrays may remain live | Immutable h plus one F-sized binary64 scatter array: 16F payload bytes total |
| Scalar high-precision work | Factor endpoint scatter and original residual evaluation | Row enclosures, weighted row/output ledgers, then factor residual/rounding ledgers |
| Source row state | Full row in current reader | Still full row unless a new bounded replay layout is built |
| Certificate tightness | Direct actual-output residual | Includes conservative scatter-rounding and publication terms; can reject when direct certifier passes |
| Large-factor sensitivity | Precision/range and available memory | gamma_f grows with s_f, creating an explicit admission/accuracy limit |
| Solver choice | All existing controls | Equally available to all compatible factor-state solvers |

The proposal removes an O(F) count of expensive interval objects, not the O(F) binary64 state itself. At small F interpreter/library overhead may dominate. At huge F even 16F bytes can violate a user's budget. At a near one, the factor a/(1-a) magnifies all errors. No unconditional 4 GB promise, speedup percentage or terabyte-to-gigabyte reduction is justified.

## 8. Rubber-Duck Corrections

| Tempting shortcut | Why it is wrong | Required correction |
| --- | --- | --- |
| Bound only T(h)-h in ordinary floating arithmetic | Arithmetic could make the residual appear smaller than it is | Enclose row arithmetic and charge scatter rounding |
| Use sum of row errors without weights | One row affects multiple factors with different output weights | Charge exactly `(d_i+q_i)*e_i` for this source model |
| Use gamma times the rounded sum without justification | It does not follow from the displayed standard-bound inversion, although stronger published bounds can justify it | Use the proved inverse formula or explicitly cite and implement the tighter theorem |
| Allow signed yhat and retain the positive-sum formula | Cancellation invalidates Q<=z/(1-gamma) | Refuse or switch to an independently justified signed-sum scheme |
| Certify ideal X(h), ignore emitted rounding | User receives xhat, not X(h) | Include Epub for every active and isolated output |
| Ignore a near one or giant factors | Error amplification and gamma can destroy admission | Explicit range checks and tolerance refusal |
| Claim fewer Decimal arrays means less whole-job RAM | Builder, solver workspace, decoder and page cache may dominate | Measure the full admitted lifecycle and phase maxima |
| Give only our candidate the new certifier | Established CG could obtain the same saving | Apply the shared publication path to all compatible strong controls |
| Fall back to the old certifier without reservation | Fallback can itself violate the promised cap | Reserve it, use an admitted blocked fallback, or refuse |

## 9. Verification-First Next Steps

1. Ask for an independent mathematical challenge of (2)-(4), including isolates, signed h with nonnegative yhat, duplicated groups, near-zero row values and damping limits. Separate proof review from code audit.
2. Add failing tests against the existing exact expanded-graph Fraction oracle. Require the new bound to dominate actual L1 error of the exact emitted binary64 values. Include a deliberately damaged output/error term that the reference certifier rejects.
3. Test every admission boundary: nonfinite state, negative/subnormal scatter input, overflow, inaccurate cardinalities, source/version mismatch, F=0, empty graph policy, truncated output and insufficient fallback reservation. Synthetic metadata can exercise gamma refusal without constructing a huge graph, but must not masquerade as a validated real source.
4. Implement the certificate as a separate module. Freeze the current solver/certifier files and all public receipts. Keep each existing solver as a control; do not retune its convergence tolerance to make the new path look faster.
5. Retain both actual-error and admission comparisons. A new conservative bound may validly reject an answer accepted by the direct certificate. Quantify rejection and extra iterations, not just speed of accepted examples.
6. Measure isolated serial paired sessions after tests finish, with full output and setup charged. Record phase RSS or allocation evidence, total worker RSS, source passes, output bytes, fallback costs and total session time. Include larger native F/N regimes only with lawful source access and a separately priced importer.
7. Establish precise nearest-art positioning for fused residual checks, streaming validated linear algebra and low-memory error estimation. The Higham summation foundation and standard residual bound are already credited. A useful implementation does not by itself resolve publication priority.

## Final Synthesis

The most credible next A01 advance is not another solver label. It is a proof-carrying publication path whose numerical guarantees do not recreate the memory problem it is meant to solve. The scalar ledger has a reviewed conditional error bound and a separate implementation removing high-precision factor arrays. The experiment record controls what may be claimed about that implementation. Whole-job physical RAM benefit and scientific distinctiveness remain open; stronger numerical precedent must remain part of the comparison.

## 10. Next Precision-Placement Test

The first executed scalar path cuts modeled publisher reads by 50.46% but is about 13% slower than ordinary reconstruction plus direct certification. Its row-enclosure pass still performs high-precision operations per membership. The next derived option is to move high precision from membership count Z toward row count N, while charging a rigorous row-summation error. This is a proposed schedule, not part of the frozen implementation or another claimed summation invention.

For each row, sequentially compute two binary64 scalars in the same order: `Hhat=sum h_f` and `Ahat=sum abs(h_f)`. The cited Rump theorem gives an enclosure radius `EH=(q_i-1)*u*ufp(Ahat)` for H, with q_i=1 handled exactly and arithmetic/range assumptions explicitly admitted. Form outward Decimal endpoints `Hhat-EH` and `Hhat+EH`, then perform the existing rational row reconstruction and error/publication ledger. This uses a constant number of scalar high-precision operations per row instead of per membership; it adds the row-summation error to Erow rather than ignoring it.

The important tradeoffs are explicit: signed cancellation can widen the row interval; the added uncertainty can defeat an otherwise acceptable tolerance; two binary64 additions per membership still cost work; and underflow/rounding-mode behavior must meet the theorem, not merely pass a startup flag. A positive-state specialization can reuse one sum, but cannot silently assume CG always returns nonnegative factor coordinates. Refuse or take an admitted alternative if the hypothesis fails.

Compare this with the frozen scalar implementation, the inexpensive direct path, and an outward-binary64 direct-residual implementation with solver buffers released when possible. Keep the same h and row order, report changed output bits, admission/fallback and total phase costs. In this source Z/N is about 59.5, which motivates testing precision placement; it does not supply a predicted speedup factor. Any contribution would have to lie in a useful complete resource/accuracy tradeoff and its graph-source integration, not in claiming the inherited row-sum inequality as new.

Implementation update, 2026-09-21: this proposal and the binary64 direct control are now [implemented and measured separately](PageRank-Precision-Placement-Experiment.md), with the original code/receipt preserved. The new candidate has independent review, 43 focused tests and 81 certified public outputs; publication ratio 0.379 versus frozen direct, compared with 0.748 for binary64 direct. The complete high-precision operation count is O(N+F), not O(N). The stronger direct control matches the candidate's 16F selected payload, worker RSS overlaps, and the faster row schedule can lose strict-tolerance admission even on an exact-answer fixture. Those are evidence about the proposed schedule, not completion of the scientific-contribution or physical-resource claim.

# Frozen Cancellation-Cap Extension

Date: 2026-09-21. Secondary protocol frozen after the degree-only native gate
and before executing the cap extension. This is not a replacement for the
negative result in [Native Results](Communities-Residual-Native-Results.md).

## Fixed Comparison

Replay exactly the 140 rows and five candidates in native receipt
`53dcc226a8b176760a1ccf7ec2d10f2572976feb5e6a4a6cdf9efda93d060b64`.
Validate every reconstructed candidate digest. No new candidate producer,
partition initialization, edge ordering, gamma, or counter budget is allowed.

For b>0 recover D=M/(b+1), where M is residual edge mass. The original fixed
capacity weighted-MG builder guarantees every residual pair has weight <=D.
Apply only the primary O(K) two-pass formula in the
[independent review](Communities-Cancellation-Cap-Review.md), section 4. Do not
use the optional sorted-prefix tier or an exact quadratic LP. For b=0 retain
the old degree-only interval. Preserve original degrees and total mass.

## Outcomes Recorded

- All candidate bounds must contain the frozen exact original scores and must
  refine the previous interval. A single violation stops the extension.
- Separate oracle from operational candidates, including baseline-only cases.
- Record both endpoint improvements, width reductions, and selected labels.
- Choose the operational winner by largest capped lower bound with the same
  baseline/matching/sweep tie order. Compare its actual quality with the old
  selected candidate. A changed label is not itself a quality improvement.
- Retain finite-family regret certificates for the three operational
  candidates. Tighter certificates are not a global optimization guarantee.
- Report coefficient/arithmetic and retained-state charges. There are still
  no measured physical-RAM, elapsed-time or storage-I/O results.

Stop promotion if no useful decisions or sufficiently sharp nontrivial
certificates improve. Do not expand to more engineered graphs to conceal
that result. Every case remains in the receipt, including b=0 and b=q.

Results belong in a separate file so this protocol remains hashable unchanged.

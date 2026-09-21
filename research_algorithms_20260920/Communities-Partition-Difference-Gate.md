# Frozen Joint Partition-Difference Gate

Date: 2026-09-21. New certificate, unchanged native candidates. Protocol
frozen before implementation results; do not reinterpret scalar width gains
as better clustering. This extends the completed cancellation-cap gate.

## Input And Controls

Use the identical 140 native rows and five candidates from receipts
`53dcc226a8b176760a1ccf7ec2d10f2572976feb5e6a4a6cdf9efda93d060b64`
and `19c522eb9167748e8acf18b7bda3255cdde7d6d44d4b7287d6fe33c7a40dd655`.
Reconstruct and verify all candidate and starting-partition digests. No new
candidate producer, graph, order, counter capacity or gamma is allowed.

For each of the 20 ordered distinct candidate-name pairs per row, compare:

1. Difference of the existing independent capped score intervals.
2. The same interval intersected with refinement/identical-partition identities.
   If A refines B, omitted internal(A)-internal(B)<=0; if B refines A,
   the difference is >=0. Equivalent partitions have difference exactly zero.
3. The above strongest simple control intersected with the proposed signed
   per-vertex residual bound. Do not attribute elementary self-cancellation or
   partition-refinement gains to the new bound.

For b>0 recover residual pair cap D=M/(b+1) from the actual fixed-budget
builder. For b=0 use the universally valid cap D=M, not a claimed MG
cancellation identity. M is omitted edge mass, not original edge mass.
Original degrees, original total W, original gamma and retained-edge score
differences determine the final objective. The residual has no unknown loops.

## Proposed Core

For candidate partitions A and B, t_i=min(r_i,D) and T=sum t_i. Aggregate
clipped sums by A community, B community and occupied (A,B) intersection cell.
There are at most K occupied cells. Define positive, negative and neutral
incident-capacity bounds

```text
p_i = T_A(i) - T_intersection(i)
n_i = T_B(i) - T_intersection(i)
z_i = T - t_i - p_i - n_i
U(A,B) = 1/2 * sum_i [min(r_i,p_i) - max(0,r_i-p_i-z_i)]
L(A,B) = -U(B,A)
```

The leading proof obligation is that these enclose the true signed omitted
mass for every feasible capped completion. The routines check only necessary
metadata conditions; they do not solve general capped degree feasibility.

## Outcomes And Stop Rules

- Every bound must contain the frozen exact score difference, and the new
  intersection must refine the strongest control. One failure stops the run.
- Report all 2,800 pair cases, with the 840 operational ordered pairs separate
  from pairs involving oracle-generated candidates.
- The original winner stays fixed. Its regret bound within the three
  operational candidates is max(0, max upper(other-winner)); self-comparison
  is excluded in every variant. Selection is not changed by this study.
- Count additional exact best-of-three certificates and newly crossed absolute
  regret thresholds 0.001, 0.01 and 0.05. These thresholds are descriptive,
  not customer requirements or global approximation factors.
- Report positive improvements over refinement controls on crossing partitions,
  not merely relabeled duplicates or nested partitions. Keep failures and
  baseline-only rows. No physical-memory or timing measurements are made.
- The existing exact replay control remains available: one paid input pass can
  score the whole fixed family exactly with no retained edge map.

If the new bound improves only trivial comparisons or does not avoid useful
verification scans, stop promoting it. Stronger implicit-LP or adaptive-search
work needs a separate, explicit contribution claim and resource accounting.

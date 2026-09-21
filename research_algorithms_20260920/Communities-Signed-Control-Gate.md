# Signed Community Controls: Frozen Attribution Gate

Date: 2026-09-21. Freeze this protocol before observing new native results.

Question: do the 20 extra operational-family certificates from the row
comparison require the MG pair cap or forced-negative-mass relaxation, or do
ordinary support/degree bounds already explain them?

Keep all 140 native cases, original sources, P0, budgets, orders, gamma, five
candidate labels, exact scores and selected operational winner from the pinned
partition-difference/native receipts. Do not generate new candidates or tune
the case set. Evaluate all 2,800 ordered distinct-name pairs, with operational
and crossing flags. Preserve every case, including no improvements.

The common starting control is the frozen capped marginal plus refinement
interval, including its original MG-cap information. The following ablation
changes only the additional signed-comparison term. It therefore isolates the
value of MG information in that term, not in construction or marginal control.

1. Support: for each sign, collect vertices incident to at least one possible
   pair of that sign, using partition/cell cardinalities, not arbitrary label
   differences. Its signed total is at most min(sum residual degrees/2,
   sum residual degrees minus their maximum). Ignore forced opposite mass.
2. Sign-only, D=M: clipped class incident caps, no forced opposite mass.
3. Row, D=M: the old joint formula with only a generic valid pair cap.
4. Endpoint, D=M: same positive upper but enforce both endpoints; subtract
   max(sum forced-negative/2, maximum forced-negative), and reverse for lower.
5. Sign-only, D=M/(b+1) for b>0, otherwise D=M.
6. Row, same MG cap: must reproduce frozen joint intervals exactly.
7. Endpoint, same MG cap: must dominate the frozen row interval.

For sign-only, use sum min(r_i,p_i)/2 and its reversed counterpart. For
endpoint use min(sum a_i/2, sum a_i-max a_i), where a_i=min(r_i,p_i).
All comparisons use original mass, retained offsets and degree penalties.
Source feasibility is witnessed by the actual residual; none of these
formulas is a complete capped-degree feasibility test.

For each tier intersect with the common control. Check direct original-score
containment, reverse signs and required dominance. Report counts of narrowed
pair intervals and fixed-winner regret-zero certificates within the three
operational proposals. Do not translate certificate counts to new executed
source savings: the prior executor was measured only for its stated policies.

Record source/test/protocol hashes and prior receipt hashes in a new exclusive
JSON receipt. Tests precede implementation and cover the independent review's
five-node counterexample, support invariance, random feasible capped residuals,
zero mass, cap monotonicity and invalid metadata. No timing/RSS measurement.
This gate alone does not compare strongest sorted scalar/meet/join controls,
exact joint LPs, arbitrary dual potentials or improved candidate discovery.

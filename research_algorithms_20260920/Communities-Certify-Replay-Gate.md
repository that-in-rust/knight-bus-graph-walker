# Frozen Certify-Or-Replay Execution Gate

Date: 2026-09-21. Follow-up after the joint native difference result. This is
an executed source-access policy, not a disk, latency or physical-RAM benchmark.

Use the existing 140 rows and three operational candidates unchanged. Validate
their labels and exact scores against the frozen native receipts. Candidate
generation and source preparation stay outside this kernel and remain paid.

Run four policies per row:

1. Independent capped intervals, then exact replay if undecided.
2. Independent intervals plus refinement, then exact replay if undecided.
3. Refinement first, joint certificate only for unresolved comparisons, then
   exact replay if still undecided.
4. Policy 3 with an exact selected modularity requirement. If the preferred
   candidate's score interval is not a point, directly replay; a selection
   certificate alone does not satisfy this stronger output contract.

Choose the proposed winner by maximum capped lower bound, preserving the
baseline/matching/sweep order on ties. On exact replay, preserve that proposed
winner if it ties for best exact score, otherwise choose the first exact best
candidate in declared order. The promise is exact best value within the fixed
family, not a global optimum or parity with Neo4j/GDS.

The source factory must refer to the same immutable graph snapshot as the
summary. A certified path must not invoke it. A fallback invokes it exactly
once and scores all candidates together with the existing exact replay kernel.
Use actual iterator reads as the logical access counter; no modeled reads may
be substituted for executed fallback. Record core invocations, comparisons,
source opens, records and label-pair checks, complete selected label digests,
and score intervals. Native full source/oracle residency prevents a RAM claim.

Unit gates must also include indistinguishable summaries whose best candidate
differs, to exercise genuine fallback; exact-score-required output; and a
crossing-partition witness that avoids source access only with the joint bound.
The native cohort may all certify under policy 3; it is not universal success.

Compare complete outputs with frozen exact scores for every run. Preserve all
baseline-only and full-budget cases. No changes to frozen probes/receipts.

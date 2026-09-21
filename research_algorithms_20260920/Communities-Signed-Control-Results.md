# Signed-Control Ablation: Simple Support Explains The Native Gain

Date: 2026-09-21. Decision: do not promote the old row formula as a distinct
native advantage. This is a useful negative attribution result, not a claim
that its inequalities are wrong or that no graph certificate can be valuable.

## Matched Results

Executed the [frozen gate](Communities-Signed-Control-Gate.md) on all 140 cases
and all 2,800 fixed ordered comparisons. Every tier contains the exact original
score difference. The MG-capped row tier reproduces every frozen joint interval
exactly. The original capped marginal/refinement control is common to all
tiers; only the additional signed term changes.

| Additional signed control | Narrower pair intervals | Narrower operational pairs | Narrower crossing pairs | Best-of-three certificates |
| --- | ---: | ---: | ---: | ---: |
| Degree mass on signed-support vertices | 552 | 86 | 498 | 140/140 |
| Sign incident caps, generic D=M | 772 | 106 | 670 | 140/140 |
| Row relaxation, generic D=M | 772 | 106 | 670 | 140/140 |
| Endpoint strengthening, generic D=M | 772 | 106 | 670 | 140/140 |
| Sign incident caps, MG-derived D | 838 | 116 | 702 | 140/140 |
| Row relaxation, MG-derived D | 838 | 116 | 702 | 140/140 |
| Endpoint strengthening, MG-derived D | 838 | 116 | 702 | 140/140 |

The support control merely finds vertices incident to a potentially positive
or negative pair using partition equivalence, then bounds that sign's total
mass by both half its endpoint degree sum and the sum excluding the largest
endpoint degree. It does not use the tighter MG cap in its signed term or
force negative incident mass. Its own computation is O(K) logical work/state.

All 20 certificates beyond the previous capped-marginal/refinement control
are already explained by this simple support bound. Thus the native result
does **not** require the proposed row relaxation, the new endpoint improvement,
or the tighter MG cap in the signed term. This does not remove the MG sketch
used for proposals or the common marginal control.

Exact receipt comparisons, not only equal totals, show:

- Generic sign-only and generic row intervals are identical on all 2,800 pairs.
- Capped sign-only, capped row and capped endpoint intervals are identical on
  all 2,800 pairs after intersection with the common control.
- Generic and capped row intervals differ on 582 pairs, but none of those
  differences yields an additional fixed-family winner certificate.

This illustrates why narrower intervals alone are not the product objective.
The tighter certificate must change an admission, source pass, candidate
quality, or another useful outcome under paid costs.

## How To Read The Earlier Execution Result

The [earlier executor](Communities-Joint-Native-Results.md) really avoided 20
verification scans relative to capped marginals plus refinement. That remains
valid historical evidence. This new gate shows the associated decisions can
also be proved by a simpler control. We have not rerun a support-only executor,
so the new receipt reports certificates, not new measured iterator accesses,
elapsed time, peak RAM or source bytes. There is no reason to claim a unique
operational advantage for the more complicated row method on this cohort.

Exact-score output still requires the previously observed 70 replays; selecting
a winner and publishing its exact score are different contracts. No proposal
changes and no global community-quality result follows.

## Surviving Mathematical Question

The independent five-node example still separates the row formula from
stronger scalar/refinement/meet/join controls, and the endpoint bound improves
that example further. Those are constructed mathematical separations, not
evidence of native workload prevalence or historical priority.

The next candidate is [implicit global dual evaluation](Communities-Implicit-Partition-Dual.md):
can we cheaply certify a genuinely unresolved decision while enforcing more
global consistency, without materializing a dense pair LP? Independent review
and dense-arithmetic tests are required. Do not reuse this already saturated
three-candidate cohort to manufacture an improvement. A later native protocol
must freeze unresolved decisions and account for paid search rounds and the
ordinary exact replay alternative.

## Reproduction

```sh
cd research_algorithms_20260920/experiments
/Users/amuldotexe/.local/bin/python3.11 -B -W error -m unittest test_signed_control_ablation -q
/Users/amuldotexe/.local/bin/python3.11 -B -W error probe_signed_control_ablation.py --output /tmp/community-signed-control-new.json
```

The output path must not already exist. Five explicit missing-implementation
assertion failures preceded implementation; all five tests pass, including
800 seeded feasible residual/partition comparisons, cap monotonicity, reverse
signs, equivalent-label cancellation and the review's strict endpoint witness.

Receipt: [JSON](evidence/community-signed-control-20260921/receipt.json), SHA-256
`7fc2d1d7b90d7113d6ce52d35672e1f6b17666357667bda49956f87f0656a440`.
Protocol SHA-256: `c7301b3eda0a3f118023ee8b20f35785d19481b69c7ce6ba4dbce48a131a50bb`.
Source/test hashes and prior receipt pins are inside the new receipt. Earlier
frozen sources, protocols and receipts are unchanged.

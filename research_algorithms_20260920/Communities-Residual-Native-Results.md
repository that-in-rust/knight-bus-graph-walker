# What The Native Residual-Quotient Gate Actually Shows

Date: 2026-09-21. A04 follow-up to the
[residual-degree representation](Communities-Residual-Degree-Envelopes.md).
The [pre-run protocol](Communities-Residual-Native-Gate.md) is left unchanged
so its original hash remains reproducible. Results are kept here instead of
appending to that frozen file.

## Decision First

The new coupled degree envelope is mathematically sound, but this native gate
does **not** establish a useful incremental community-selection contribution.
Weighted-MG underestimates with original degrees already certify all the
operational gains observed here. Preserve the theorem and negative result;
do not optimize its implementation or call its current form paper-ready.

This is not a rejection of bounded-memory community detection or of every
possible use of the interval. It rejects attributing these native gains to
the newly derived coupling formula.

## What Was Executed

Four projections, three resolutions, two edge orders and a deduplicated
six-position budget schedule produce **140 rows**, not 144: two small Karate
quotients have coincident rounded budget positions. Each row scores three
summary-only candidates and two separately marked exact-quotient candidates:
**700 exact original-objective containment checks**. Every check passed.

The operational producer uses only retained pairs, original degrees, and total
mass. Its two proposals are a disjoint positive-gain matching and one ordinary
local-move sweep. Both use the original objective's null-model penalty, not a
renormalized sparse-graph objective. The exact quotient's proposals never
enter the operational selection; they are eligibility/oracle controls.

At gamma=1, one original-node sweep creates these genuinely many-community
starts on the two larger native inputs:

| Projection | Original nodes | Original edges | Base blocks K | Exact non-loop coarse pairs q | Base modularity |
| --- | ---: | ---: | ---: | ---: | ---: |
| GrQc | 5,242 | 14,484 | 1,801 | 3,465 | 0.592409 |
| Facebook | 4,039 | 88,234 | 569 | 2,458 | 0.617536 |
| Karate weighted | 34 | 78 | 11 | 20 | 0.289209 |
| Karate unit | 34 | 78 | 13 | 27 | 0.217045 |

These are not customer workloads, semantic ground truth, or GDS equivalence
tests. They are fixed native topologies with a declared heuristic start.

## Attribution Results

| Test | Result | Meaning |
| --- | ---: | --- |
| Strictly positive certified gain over P0 | 94/140 rows | A bounded retained-edge representation can still propose useful coarsenings on this cohort |
| Winner changed by adding residual-degree lower bound | 0/140 | No measured operational selection benefit from the new lower bound |
| Operational candidate lower bound improved | 0/420 | Every certified operational gain was already obtained by known retained-edge underestimates |
| Any candidate lower bound improved | 11/700 | All eleven are oracle-generated exact-sweep partitions on weighted Karate, not operational wins |
| Coupled upper sharper than independent-group upper | 44/700 | Some genuinely tighter mathematical intervals, mostly on oracle candidates |
| Operational coupled upper improvement | 2/420 | Both are small unit Karate; none on GrQc or Facebook |
| Forward/reverse gain differs | 37/70 paired cases | Order sensitivity survives; the maximum absolute gain difference is 0.086543 |

The two operational upper improvements are gamma=1/2 and gamma=1 on
forward-order unit Karate, b=14. Width reductions are 5/78 and 1/156,
respectively. They do not change the selected candidate.

An exact baseline interval is not evidence that a good community result was
found: returning unchanged P0 gives zero uncertainty even at b=0.

## Quality Versus Counter Budget

These are forward order, gamma=1. Values are **absolute modularity points**,
not percentages or speed ratios. Every selected nonbaseline entry here is the
retained-edge sweep. The last column compares with the best of the two exact
quotient proposals, not with the optimum or with Neo4j/GDS.

| Projection | b / q | Actual improvement | Certified improvement | Selected score interval width | Gap to exact-quotient proposal |
| --- | ---: | ---: | ---: | ---: | ---: |
| GrQc | 0 / 3,465 | 0 | 0 | 0 | 0.184018 |
| GrQc | 35 / 3,465 | 0.004597 | 0.001559 | 0.009873 | 0.179421 |
| GrQc | 174 / 3,465 | 0.021997 | 0.011572 | 0.048882 | 0.162021 |
| GrQc | 693 / 3,465 | 0.065660 | 0.035281 | 0.101526 | 0.118358 |
| GrQc | 1,733 / 3,465 | 0.131997 | 0.088018 | 0.139533 | 0.052021 |
| GrQc | 3,465 / 3,465 | 0.184018 | 0.184018 | 0 | 0 |
| Facebook | 0 / 2,458 | 0 | 0 | 0 | 0.172961 |
| Facebook | 25 / 2,458 | 0.045278 | 0.032357 | 0.032272 | 0.127683 |
| Facebook | 123 / 2,458 | 0.089935 | 0.062723 | 0.071271 | 0.083026 |
| Facebook | 492 / 2,458 | 0.152477 | 0.117717 | 0.070744 | 0.020484 |
| Facebook | 1,229 / 2,458 | 0.168637 | 0.149721 | 0.037469 | 0.004324 |
| Facebook | 2,458 / 2,458 | 0.172961 | 0.172961 | 0 | 0 |

For Facebook in this one declared scenario, a 20%-of-pairs counter budget
preserves much of the tested exact-proposal improvement. GrQc preserves much
less. This is a meaningful candidate-quality tradeoff, but weighted sketches
for coarse aggregation are already prior art. See
[Sahu, arXiv:2411.02268v1, section 4.1](https://arxiv.org/html/2411.02268v1).
Our global canonical-pair budget and original-objective certificates differ
from that paper's per-community sketch construction; the paper's empirical
ratios and our ratios are not interchangeable.

Neither small b nor small selected width alone is success. More counters can
produce a substantially better but less tightly certified selected partition,
so selected width need not decrease monotonically with budget.

## Why The Extra Bound Usually Did Not Help

The formulas themselves predict the limitation. Write residual mass as M,
community residual volume as s_C, largest residual vertex degree in C as a_C,
and ell_C=max(0,2*a_C-s_C).

```text
Extra lower information:
  L = max(0, max_C s_C - M)
  positive only when one community holds > half the residual degree.

Independent-group upper:
  U_group = M - sum_C ell_C / 2

Coupled upper improvement:
  U_group - U = max(0, max_C ell_C - sum_C ell_C / 2)
  positive only when one community's forced external degree dominates.
```

Thus this coupling is aimed at a particular imbalance, not uniformly sharper
small-community discovery. The original star witness concentrated exactly
that imbalance. Keeping it as a proof witness was correct; treating it as a
representative native advantage would have been incorrect.

## The Stronger Simple Control: Replay

Once a small candidate family is fixed, exact scoring does not require an
edge-sized quotient. Keep its labels, stream the original coarse contributions
once, accumulate internal weights and original degrees, then evaluate the
ordinary modularity formula. This is a standard direct computation, not a new
algorithm. It is implemented in `probe_quotient_replay_control.py`.

For C candidate partitions, K base blocks and T streamed contributions:

```text
Supplied candidate labels       C*K records
Degree and community scratch    O(K) exact-rational records
Internal mass accumulators      O(C) records
Retained edge records           0
Time                            O(C*T + C*K) rational operations
Source rereads                  1 sequential pass for the whole family
```

The physical scan model assumes replayable/spooled source, bounded decoding,
and resident or otherwise explicitly paid label access. The native driver
replays a resident iterator and is not a disk benchmark. Exact-rational bit
growth, initial labels, full graph preparation, output expansion and mapping
are still charged. A non-replayable stream is a different contract.

This control means the degree envelope's opportunity is to **avoid a scan**,
or cheaply examine many adaptive candidates, not to make exact evaluation
possible where it otherwise requires storing all edges. Candidate discovery
remains the harder problem; exact scoring cannot invent a missing partition.

The [separate replay receipt](evidence/community-quotient-replay-20260921/receipt.json)
is complete: all 700 scores agree exactly with the frozen quotient oracle,
over 3,703,152 contribution records and 18,515,760 label-pair checks. The five
candidate partitions were scored together for verification, including the two
explicitly oracle-generated controls. Three operational candidates alone would
need three rather than five label-pair checks per record.

Exact rescoring changed neither the operational winner nor its quality in any
of the 140 rows. Its contribution here is eliminating score uncertainty, not
discovering a better partition. The receipt SHA256 is
`e9afffd7fa898d4d638972074b251603734b5a71fdecac703e4e74e41e6ced6c`.

## Evidence And Reproduction

- Frozen protocol SHA256:
  `30156b553433cf72ebe29023f56ab2a3c937b37fb5b707570ee9d777a85831fb`.
- Native receipt: [receipt.json](evidence/community-residual-native-20260921/receipt.json).
  SHA256 `53dcc226a8b176760a1ccf7ec2d10f2572976feb5e6a4a6cdf9efda93d060b64`.
- The receipt pins five source files, prior native receipt, source data,
  initialization digests, candidate digests and all 700 exact scores/bounds.
- Seven new gate tests include 90 comparisons against a dense direct-objective
  single-sweep oracle, 120 seeded sketch candidate families, quotient/loop
  conventions, full-budget exactness, the hub witness and input ambiguity.
- Three replay tests add 500 seeded exact-score comparisons, single-pass
  iterator consumption, parallel-edge/self-loop handling and invalid inputs.
- Current focused suite: 21 tests, terminal exit zero. These are semantic
  tests, not independent code review or performance evidence.

```sh
cd research_algorithms_20260920/experiments
python3.11 -B -W error -m unittest test_residual_degree_envelope test_residual_native_gate test_quotient_replay_control -q
python3.11 -B -W error probe_residual_native_gate.py --output /tmp/residual-native-new-receipt.json
python3.11 -B -W error probe_quotient_replay_control.py --output /tmp/residual-replay-new-receipt.json
```

Receipt writers require a new path and never overwrite frozen evidence.

## Next Research Gate

Do not spend the next turn optimizing weighted-MG cancellation or repeating
this matrix. The missing capability is a useful quality decision at low
retained state, beyond the original-degree sketch and exact-replay controls.
Investigate extra residual information only with an explicit separating
theorem or a measured change in useful candidate decisions. The independent
[cancellation-cap review](Communities-Cancellation-Cap-Review.md) is complete
and lead-replayed. Its separately frozen [native extension](Communities-Cancellation-Native-Results.md)
tightens 213/700 intervals but changes no candidate or lower bound. After the
same elementary self-comparison correction, it certifies four additional
finite-family winners without a source rescan. This is a certification gain,
not a better clustering or RAM measurement. Exact external quotient construction and real cut-sparsifier
construction remain unimplemented comparison paths, not defeated alternatives.

All six other algorithm-family requirements and stop rules remain intact.
This result advances the research decision; it does not complete the user's
seven-family innovation objective.

# Joint Partition Certificates: Native Results And Limits

Date: 2026-09-21. A04 research, not a production resource claim.

## What Was Actually Executed

The frozen [pair protocol](Communities-Partition-Difference-Gate.md) compares
the same five candidate partitions in each of 140 previously fixed cases.
Three candidates are operational (baseline, retained-edge matching, retained
sweep); two are exact-quotient oracle proposals used only for comparison.
Sources, initial partitions, budgets, stream orders, gamma and candidate hashes
remain unchanged. There are 2,800 ordered distinct-name comparisons, including
840 comparisons between operational candidates. Equivalent partitions can
have different names; identity/refinement controls remove that easy gain.

All exact original-score differences lie in their reported intervals. The
joint interval improves 838 of the 2,800 pair intervals beyond the declared
independent-cap/refinement control. Of these, 702 are crossing-partition
comparisons; 72 improvements concern operational crossing pairs. These are
interval improvements, not new or better partitions.

| Fixed operational family | Independent caps prove best | Plus refinement | Plus joint row certificate |
| --- | ---: | ---: | ---: |
| GrQc, 36 cases | 16 | 24 | 36 |
| Facebook, 36 cases | 24 | 30 | 36 |
| Weighted Karate, 32 cases | 23 | 32 | 32 |
| Unit Karate, 36 cases | 24 | 34 | 36 |
| Total, 140 cases | 87 | 120 | 140 |

The improvement from 87 to 140 is **33 ordinary refinement certificates plus
20 further joint certificates**, not 53 gains attributable to the new formula.
Every selected partition was already best within these three proposals in the
earlier exact replay. No selected partition changes. This says nothing about
global modularity optimality, GDS trajectory parity or broader candidate sets.

## Real Certify-Or-Replay Control Flow

The separately frozen [execution protocol](Communities-Certify-Replay-Gate.md)
implements a lazy source factory. A successful certificate never opens it.
An unresolved comparison opens it once and scores the whole fixed family in
one pass. This is actual executed control flow, not a prediction from interval
widths. All 560 executions return complete labels with the exact best score
within the declared family, checked against the pinned original-score oracle.

| Policy | Runs | No replay | Replays/source opens | Contribution records consumed | Label-pair checks | Joint core calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Independent caps | 140 | 87 | 53 | 1,350,126 | 4,050,378 | 0 |
| Caps plus refinement | 140 | 120 | 20 | 703,368 | 2,110,104 | 0 |
| Caps, refinement, joint | 140 | 140 | 0 | 0 | 0 | 20 |
| Same joint policy, exact scalar score required | 140 | 70 | 70 | 2,466,948 | 7,400,844 | 0 |

The first three policies each make 280 candidate comparisons. The exact-score
policy makes 140: it directly replays the 70 cases whose selected score interval
is not a point. Certifying the identity of a best candidate does not reveal its
exact score. An API requiring that scalar cannot silently accept an interval.

Compared with caps plus refinement, the tested joint path eliminates 20
additional verification passes and 703,368 logical contribution-record reads,
paying 20 joint-core calls. **This is not a measured latency or RAM reduction.**
The fixture loader and candidate producers are resident, initial preparation
and output costs remain, and one hot sequential replay may be inexpensive.

## Independent Correction

The [independent mathematical review](Communities-Partition-Difference-Review.md)
is complete and its embedded checker was replayed by the lead with Python
3.11, warnings treated as errors: 21,643 exact capped-LP partition-pair cases
and 24 signed-score checks pass. The review supports validity but identifies
the mechanism as a classical separable row relaxation. A same-asymptotic-work
signed-endpoint strengthening dominates the original row formula. Stronger
scalar, meet/join and support controls also need explicit comparison.

The original native experiment did not compare those controls or isolate
generic degree information (D=M) from the tighter MG cap D=M/(b+1).
Consequently these historical counts cannot establish a distinct algorithmic
contribution over them. A separate comparator ablation is the next decision;
old receipts and source files are immutable.

## Evidence Anchors

- Pair receipt: [receipt.json](evidence/community-partition-difference-20260921/receipt.json), SHA-256 `b0683779648e2c72a630690e80047fcc815d76cc4b4af36c4a38401afa5870b1`.
- Executor receipt: [receipt.json](evidence/community-partition-selection-20260921/receipt.json), SHA-256 `1c6b2cf5b81167aa6601e9ee99adf1b2d497cc33242bab99e1dc219dac196a66`.
- Both receipts pin their source/tests, frozen protocol and prior evidence.
- Lead combined community suite: 100 tests, exit 0, observed 2026-09-21.
- No elapsed-time/RSS benchmark, independent executor-code audit, physical
  memory cap or all-seven-family completion is asserted.

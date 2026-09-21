# Cancellation Caps: Native Certificate Gains And Their Limits

Date: 2026-09-21. Primary linear cap formula, fixed native candidate family.
This follows the separately frozen [extension protocol](Communities-Cancellation-Native-Gate.md)
and the [independent proof/prior-art review](Communities-Cancellation-Cap-Review.md).

## Scientific Result

The existing weighted sketch contains more useful information than residual
degrees alone. Recovering its implied per-pair residual cap tightens original
modularity intervals on native graphs without adding retained edge counters.
On this fixed cohort it improves **certification**, not candidate discovery or
the selected clustering. Neither a runtime advantage nor paper readiness has
been established.

The distinction matters: the degree-only follow-up had no operational lower
improvement. The cap follow-up also has none, but it yields some stronger
upper certificates, including four new proofs of optimality within the tiny
three-candidate family when compared correctly. This is narrower than finding
a better community partition, proving a global approximation ratio, or
achieving a physical memory budget.

## Mechanism And Contract

The fixed-b reduce-by-minimum weighted-MG builder cancels the same delta from
b tracked pairs and one new pair. For b>=1, total cancellation D therefore
satisfies

```text
Residual mass M = (b+1)*D
Every residual pair weight <= D = M/(b+1)
```

These are established sketch facts, not a new heavy-hitter algorithm. The
review also proves that, with arbitrary rational update splitting/order,
any capped residual of this mass can be constructed from cancellation batches
and followed by the desired retained counters. Its hypersimplex decomposition
is classical. This identifies the summary's completion model under those
assumptions, not a theorem for a fixed unit-update order.

For each candidate, the prototype clips each residual degree at D, aggregates
the clipped values by community, and derives lower/upper community boundary
mass. Matching boundary mass at both ends yields an interval for omitted
internal weight. The exact formulas, proof, ambiguity pair, and non-tightness
example are in review sections 2-6. The implementation is
`experiments/probe_cancellation_cap_gate.py`.

```text
Original bounded sketch + candidate labels
                    |
       recover implied omitted-edge cap
                    v
     two degree/label aggregation passes
                    |
       bound omitted internal edge mass
                    v
   original-normalization score interval
                    |
       compare with other candidates
```

Logical scoring cost is O(K+b), with O(number of candidate communities)
scratch and the existing label/degree arrays. Rational construction and
scratch allocation are real costs. No new edge support map is stored.
The helper accepts an already certified feasible residual model; its necessary
metadata checks are not a full capped-feasibility solver. Native cap recovery
uses the actual configured sketch capacity, never final occupancy. b=0 uses
the old degree-only interval; this separate discard-all path has no asserted
cancellation history.

## Frozen Native Results

All 140 cases and 700 candidate digests match the original native receipt.
Every capped interval contains its exact original score and refines the
degree-only interval. No input, candidate policy, gamma or order was changed.

| Quantity | Result |
| --- | ---: |
| Candidate upper bounds improved | 213/700 |
| Operational upper bounds improved | 98/420 |
| Any lower bound improved | 0/700 |
| Selected candidate changed | 0/140 |
| Actual selected quality improved or declined | 0/140 either way |
| Original conservative family-regret bound improved | 49/140 |
| Cases with narrower selected score interval | GrQc 18, Facebook 24, weighted Karate 3, unit Karate 4 |

Selected score intervals, forward order and gamma=1:

| Projection | b / q | Degree-only width | Capped width | Width reduction |
| --- | ---: | ---: | ---: | ---: |
| GrQc | 35 / 3,465 | 0.009873 | 0.009873 | 0% |
| GrQc | 174 / 3,465 | 0.048882 | 0.047397 | 3.0% |
| GrQc | 693 / 3,465 | 0.101526 | 0.085750 | 15.5% |
| GrQc | 1,733 / 3,465 | 0.139533 | 0.095347 | 31.7% |
| Facebook | 25 / 2,458 | 0.032272 | 0.025347 | 21.5% |
| Facebook | 123 / 2,458 | 0.071271 | 0.045555 | 36.1% |
| Facebook | 492 / 2,458 | 0.070744 | 0.053789 | 24.0% |
| Facebook | 1,229 / 2,458 | 0.037469 | 0.029609 | 21.0% |
| Karate weighted | 10 / 20 | 0.194805 | 0.110390 | 43.3% |
| Karate unit | 14 / 27 | 0.256410 | 0.160256 | 37.5% |

These percentages describe interval width only, not RAM, runtime, modularity
improvement, accuracy of semantic communities, or a probability of correctness.
The inequalities are deterministic under their exact-arithmetic contract.

## Rubber-Duck Correction: Do Not Compete With Yourself

The frozen native drivers reported the valid but unnecessarily conservative
bound `max_P upper(P)-lower(winner)`, including the winner's own upper bound.
That can charge uncertainty to a candidate beating itself, even though its
score difference from itself is exactly zero. Preserve those historical values,
but use this stronger elementary control for decision usefulness:

```text
regret(F, winner) <= max(0,
    max_{P in F, P != winner} upper(P) - lower(winner))
```

F contains only baseline, matching, and retained-edge sweep. Independent
intervals still ignore correlations between different partitions on the same
residual graph, so even this is not generally sharp. No extra graph information
is required to remove the self-comparison; that correction is not an innovation.

Applying the same correction to both old and new bounds gives:

| Matched decision-level comparator | Degree-only | Capped |
| --- | ---: | ---: |
| Proves winner best within the three candidates | 83/140 | 87/140 |
| Cases with strictly improved corrected regret bound | baseline | 37/140 |
| Additional cases crossing regret <=0.001 | baseline | 4 |
| Additional cases crossing regret <=0.01 | baseline | 9 |
| Additional cases crossing regret <=0.05 | baseline | 2 |

These gates are descriptive absolute modularity tolerances, not user-selected
requirements. Baseline-only and full-budget rows are included in the totals,
so the 87/140 must not be presented as a broad clustering success rate.

All four new exact finite-family certificates occur on GrQc near b=q/2:

| Gamma | Order | b | Old corrected regret bound | New corrected regret bound |
| --- | --- | ---: | ---: | ---: |
| 1/2 | forward | 1,736 | 0.036192 | 0 |
| 1/2 | reverse | 1,736 | 0.024793 | 0 |
| 1 | reverse | 1,733 | 0.028358 | 0 |
| 3/2 | reverse | 1,736 | 0.030241 | 0 |

The actual winner was already correct on these cases; the improvement is
proving that fact from the smaller summary without an original-edge rescan.
For a full execution claim, price the cap evaluation and retained labels
against the exact replay control's one sequential pass. The resident study
does not make that comparison in elapsed seconds or bytes.

The corrected counts are reproducible directly from the two frozen receipts:

```python
import json
from fractions import Fraction as F
from pathlib import Path

root = Path('research_algorithms_20260920/evidence')
old = json.loads((root/'community-residual-native-20260921/receipt.json').read_text())
new = json.loads((root/'community-cancellation-native-20260921/receipt.json').read_text())
counts = [0, 0, 0]
for a, b in zip(old['rows'], new['rows']):
    assert (a['dataset'], a['gamma'], a['order'], a['budget']) == (b['dataset'], b['gamma'], b['order'], b['budget'])
    ca = next(c for c in a['candidates'] if c['name'] == a['winner'])
    cb = next(c for c in b['candidates'] if c['name'] == b['winner'])
    ra = max([F(0)] + [F(c['degree_upper'])-F(ca['degree_lower']) for c in a['candidates'] if c['operational'] and c['name'] != a['winner']])
    rb = max([F(0)] + [F(c['upper'])-F(cb['lower']) for c in b['candidates'] if c['operational'] and c['name'] != b['winner']])
    counts[0] += ra == 0
    counts[1] += rb == 0
    counts[2] += rb < ra
assert counts == [83, 87, 37]
print(counts)
```

## Evidence And Remaining Contribution Gate

- Receipt: [receipt.json](evidence/community-cancellation-native-20260921/receipt.json),
  SHA256 `19c522eb9167748e8acf18b7bda3255cdde7d6d44d4b7287d6fe33c7a40dd655`.
- It pins protocol, review, implementation and tests, and the previous native
  receipt. The review SHA is
  `838ab697f829c117313ea9c6bf7562dbaceb27f8e36a945d8a0a12ffb2aa0646`.
- Lead read the complete independent review/checker and replayed it with
  Python 3.11, exit zero: 1,172 exact LP/partition cases and 49,152 weighted
  stream-prefix checks. It imports no lead implementation.
- Five new prototype tests pass, including 1,280 score/containment checks from
  actual weighted MG residuals, the tight K4 example, a non-tight interval,
  the indistinguishability obstruction and invalid metadata.
- Full community suite: **89 tests pass**, terminal exit zero. Independent
  mathematical review is not an independent native-driver code audit.

This branch now has a concrete native certificate improvement. The next
research question should target **differences between partitions**, cancelling
agreement instead of separately bounding two absolute scores. It must beat the
self-exclusion control and preserve the exact-replay alternative. Whether that
produces a nontrivial theorem, useful adaptive candidate search, or neither is
open. Do not claim success from sharper widths on unchanged candidates alone.

An exact capped-completion LP can serve as a tiny oracle but has quadratic
potential pair variables; it is not an O(K) production procedure by default.
Native discovery at much smaller b, a stronger scientific separation, complete
workflow resource enforcement, and the other six family contributions remain
unresolved. The seven-family goal remains active.

### Concrete Next Mathematical Object

For two fixed partitions A and B, the residual difference uses only edges
internal to exactly one of them. Edges internal to both or neither cancel.
The next candidate should exploit this correlation, not subtract two separate
worst-case completions that need not coexist.

One unimplemented derivation to challenge is the following linear-work outer
bound. Let t_i=min(r_i,D), T=sum_i t_i; keep clipped-degree sums by A label,
B label, and the occupied intersection cell (A label,B label). There are at
most K occupied cells, so this does not require a dense community-pair table.
For vertex i define

```text
p_i = clipped sum in A(i) - clipped sum in intersection(i)
n_i = clipped sum in B(i) - clipped sum in intersection(i)
z_i = T - t_i - p_i - n_i

candidate upper for I_R(A)-I_R(B):
  (1/2) * sum_i [min(r_i,p_i) - max(0,r_i-p_i-z_i)]
```

p, n and z upper-bound incident residual mass on positive, negative and
neutral edge classes. Allocating degree first to positive and then neutral
classes explains the candidate inequality; reversing A/B gives a lower bound.
It should return exactly zero for identical partitions, even if both absolute
score intervals are wide. It is an elementary relaxation, not yet independently
reviewed, implemented, tested, or established as a new result. Candidate bounds
must be combined with, and compared fairly against, existing interval-difference
bounds, partition-refinement identities and exact source replay. Retained-edge
and original-degree penalty differences must be added with correct signs.

The next action is a small independent proof/oracle gate followed, if valid,
by the same fixed native pair comparisons. Only then consider an adaptive
optimizer or physical implementation. Do not transfer the current scalar
interval's evidence to this untested pairwise proposal.

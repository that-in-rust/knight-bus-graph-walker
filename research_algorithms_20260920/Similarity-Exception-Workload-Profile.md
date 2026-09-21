# Similarity Exception Workload Profile

Date: 2026-09-21. Bounded A05 logical-workload sidecar. This is the sole owned
file. No download, generated graph/index, timing, RSS measurement, production
integration, broad validation suite, commit, or push is part of this diagnostic.

## Bounded Verdict

**Do not promote fixed tau exceptions as a general memory improvement on these
workloads.** The G=4 envelope is genuinely stronger than union for some admitted
calls, but most of that advantage is already captured by the existing interval
filter. At tau=8 only 105 of 12542 scored calls improve on that filter. They are
logical opportunities, not measured pruning or savings.

**The modal extension survives this check and materially changes the storage
conclusion.** In the G=64 sensitivity, e=G everywhere at tau=8, yet modal d means
are 8.790 (GrQc) and 27.847 (Facebook). That warrants a conditional modal-storage
investigation, not full integration on this evidence alone: no large-G scores
were evaluated, r grows, and this is neither a layout nor a memory benchmark.
An adaptive choice of dense versus compressed metadata and a bounded-r policy
remain necessary questions, not implemented recommendations in this sidecar.

Independent math supports clipped-signature exactness, d<=e and local behavioral
minimality. It does not establish a new codec or audit the lead implementation.
No lead floor/modal/exception compiler or tests were read or run.

## Scope And Denominators

The frozen inputs are the existing ca-GrQc and Facebook gzip sources and the
30 source IDs in each `Similarity-Overlap-{GrQc,Facebook}-Results.json` receipt.
The diagnostic does not resample queries or select them by outcomes. Both
receipts use G=4 and block size two. Normalization uses the exact old parser;
physical rows are ordered by original node ID, consecutive rows form blocks,
and feature group is `original_feature_ID % G`. Capacities are maxima of each
row's subtree counts, not sums of leaf maxima. H_j counts distinct union
features once. Query q_j counts source features in that union, once per feature.

The read-only reproduction follows the code in
[Similarity-Overlap-Query-Integration.md](Similarity-Overlap-Query-Integration.md),
`experiments/bench_selective_capacity_similarity.py`,
`experiments/probe_selective_capacity_similarity.py`, and the old laminar builder.
The canonical writer preserves the supplied target order. No native floor or
exception implementation is imported or examined.

- **All full pairs:** every physical two-row block, once, independent of queries.
- **Queried full pairs:** distinct full pairs reached with Q>0 by at least one
  frozen source not contained in that pair. This is not a query-frequency weight.
- **Positive nonself calls:** source/full-pair combinations with Q>0 and neither
  row equal to the source ID. This is the main query-frequency denominator.
- **Self-containing pairs:** retained in physical profiles, but their query calls
  are counted separately and excluded from main score/eligibility summaries.
  This avoids crediting a metadata envelope containing the disallowed self row.
  It also excludes the other row in that pair from the main diagnostic. The
  actual old query keeps the pair metadata and excludes self only during body
  scoring; this diagnostic is not replaying the complete top-k schedule.
- **Singleton tails:** counted separately, never treated as a two-row core.
- **Q=0 calls:** recorded for all-pair coverage, but excluded from positive-call
  eligibility fractions; they are vacuously tau-admitted and have score zero.

For tau in 1,2,4,8, exception means `l_j=H_j-C_j<tau`; e counts all G original
leaves, including zero-population groups. Admission is `max_j q_j<=tau` (q<=H
and Q<=a already hold by construction). An admitted call is **active** if some
positive q_j exceeds l_j. Such a leaf is necessarily an exception. Active is
necessary, not sufficient, for a bound stronger than ordinary union/length.
Inactive admitted calls have the proven floor-collapse score even when a coarse
component floor would reject them.

The diagnostic reports r, core node count K, maximal owner runs R, e, sparse
support s, modal deviations d, and J, the number of components owning at least
one original leaf. For fixed exceptions the auxiliary-entry proxy is R+e;
for modal signatures it is R+d+J, explicitly charging the component defaults.
Both are compared with the old G-entry leaf table, holding shared core metadata
aside. Defaults might instead be extra fields in existing core records; charging
them as separate entries is conservative, not a required layout. Run, default,
and deviation records have different fields, and clipping changes integer
widths. These entry counts are not bytes or physical RAM; even a lower entry
count alone would not establish a resident-memory improvement. In JSON,
`aux_less_G` means R+e<G and `modal_aux_less_G` means R+d+J<G.

G=4 is the receipt-reproducing primary profile. G=16,64 are bounded counts-only
sensitivity profiles: the same physical pairs, same 30 sources, and the same
original-ID modulo rule, with only its modulus changed. They do not produce
new sources, serialized metadata, indexes, or benchmark receipts. No exact
orientation evaluator is called for these larger G values. Every static profile
includes zero-population original leaves rather than silently removing them.

## Recorded Results

All numbers below come from the retained successful run. Percentages and means
are rounded; exact integer histograms, per-source counts, input hashes, helper
hashes, and per-block distributions are preserved in the stdout appendix.

### Primary Coverage

| Dataset | Nodes / undirected edges | Full pairs | Distinct queried pairs | All source/full-pair calls | Positive nonself calls | Positive self calls | Q=0 full-pair calls |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GrQc | 5242 / 14484 | 2621 | 819 | 78630 | 1407 | 29 | 77194 |
| Facebook | 4039 / 88234 | 2019 | 1889 | 60570 | 12706 | 30 | 47834 |

Facebook has one singleton tail: all 30 source/singleton calls have Q=0 and none
is a self call. GrQc has no singleton. Each dataset has 30 self-containing full
pair calls; one GrQc source has degree zero, so only 29 are positive. Positive
self calls are excluded from every following query table and reported separately
in stdout. Four GrQc source IDs have no positive nonself call; all 30 Facebook
sources do. Primary posting counters reproduce the frozen receipts exactly:
GrQc 1436 reached blocks / 7237 block memberships; Facebook 12736 / 155786.
Here those posting counts include self-containing blocks as the old path does.

Physical overlap is reproduced too: GrQc has 2564 disjoint pairs of 2621, with
160 shared memberships in total; Facebook has 3 disjoint pairs of 2019, with
12651 shared memberships. This large difference matters for the floor profile.

### Primary Structure

Histogram entries below correspond to r=0,1,2,3 or s=1,2,3,4, as labelled.

| Dataset | All-pair r histogram | Queried-pair r histogram | Positive-call weighted r histogram | Positive-call s histogram |
| --- | --- | --- | --- | --- |
| GrQc | 1412, 1049, 153, 7 | 539, 238, 40, 2 | 993, 341, 70, 3 | 881, 270, 94, 162 |
| Facebook | 1539, 344, 125, 11 | 1450, 313, 116, 10 | 9865, 2028, 736, 77 | 6562, 2069, 1260, 2815 |

For static e,d histograms the five entries are counts at 0,1,2,3,4.
The last column gives exact e and d sums over distinct queried pairs (819 GrQc,
1889 Facebook), not query-weighted sums. Empty groups count as leaves here.

| Dataset | tau | All-pair e histogram | All-pair d histogram | Queried-pair sum e / sum d |
| --- | ---: | --- | --- | ---: |
| GrQc | 1 | 123, 242, 598, 1110, 548 | 448, 1121, 1052, 0, 0 | 1776 / 958 |
| GrQc | 2 | 23, 49, 91, 390, 2068 | 262, 970, 1212, 177, 0 | 2824 / 1236 |
| GrQc | 4 | 2, 4, 14, 34, 2567 | 195, 860, 1158, 408, 0 | 3215 / 1470 |
| GrQc | 8 | 0, 0, 1, 2, 2618 | 187, 810, 1088, 536, 0 | 3273 / 1584 |
| Facebook | 1 | 1313, 269, 192, 148, 97 | 1438, 401, 180, 0, 0 | 1272 / 679 |
| Facebook | 2 | 916, 292, 242, 229, 340 | 1053, 529, 423, 14, 0 | 2478 / 1276 |
| Facebook | 4 | 480, 185, 217, 266, 871 | 606, 533, 714, 166, 0 | 4418 / 2253 |
| Facebook | 8 | 171, 93, 103, 133, 1519 | 278, 492, 744, 505, 0 | 6254 / 3261 |

### Primary Query Frontier

Admission/refusal denominator is every positive nonself call: 1407 GrQc or
12706 Facebook. "Active / admitted" is the requested fraction with at least one
positive exception q>l; the next column also supplies the all-positive-call
fraction. A refusal only means max(q)>tau, not incorrect metadata or an invalid
original query. All admitted calls satisfy the signature-based validity test
because q was derived from the actual block union.

| Dataset | tau | Admitted / refused | Active / admitted | Active / all positive |
| --- | ---: | ---: | --- | --- |
| GrQc | 1 | 1103 / 304 | 503/1103 (45.6%) | 503/1407 (35.7%) |
| GrQc | 2 | 1225 / 182 | 600/1225 (49.0%) | 600/1407 (42.6%) |
| GrQc | 4 | 1284 / 123 | 648/1284 (50.5%) | 648/1407 (46.1%) |
| GrQc | 8 | 1314 / 93 | 678/1314 (51.6%) | 678/1407 (48.2%) |
| Facebook | 1 | 7824 / 4882 | 1657/7824 (21.2%) | 1657/12706 (13.0%) |
| Facebook | 2 | 9474 / 3232 | 2110/9474 (22.3%) | 2110/12706 (16.6%) |
| Facebook | 4 | 10459 / 2247 | 2522/10459 (24.1%) | 2522/12706 (19.8%) |
| Facebook | 8 | 11228 / 1478 | 3032/11228 (27.0%) | 3032/12706 (23.9%) |

Each G=4 stdout profile also reports, across **all** full pairs, histograms of
how many of the 30 sources yield an admitted, refused, or active nonself positive
call at that pair. Thus a zero in an admission histogram can mean unreached or
only refused; it is not automatically a storage failure. These histogram sums
and first moments are asserted against pair counts and query totals. The
per-source rows preserve all frozen IDs and their outcomes; they are not new
samples. Each admitted-set e_sum/d_sum can be divided by admitted to recover a
query-weighted storage profile, separately from static all/queried profiles.

### Primary Logical Scores

These counts are strictly tighter scores among admitted positive nonself calls,
not actual pruning events. Counts for different tau are nested and must not be
summed as independent exact evaluations. Every tau=8-admitted call is evaluated
once; smaller tau reuse the result.

| Dataset | tau | Exact < old union | Exact < two-size union | Exact < existing interval | Interval-tight and R+e<G | Interval-tight and R+d+J<G |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GrQc | 1 | 415 | 354 | 49 | 1 | 0 |
| GrQc | 2 | 489 | 381 | 57 | 0 | 0 |
| GrQc | 4 | 531 | 385 | 59 | 0 | 0 |
| GrQc | 8 | 559 | 385 | 59 | 0 | 0 |
| Facebook | 1 | 88 | 40 | 2 | 0 | 0 |
| Facebook | 2 | 274 | 146 | 14 | 0 | 0 |
| Facebook | 4 | 498 | 276 | 31 | 0 | 0 |
| Facebook | 8 | 831 | 423 | 46 | 0 | 0 |

At tau=8, the 12542 exact calls split into 3710 active and 8832 inactive calls;
every inactive call equals both union controls. There are 1390 improvements over
the old union control, 808 over two-size union, but only 105 over the existing
interval filter (59 GrQc, 46 Facebook). The interval filter was already part of
the old pipeline, so an old-union comparison alone overstates the incremental
case. These calls include ones a real pipeline may reject earlier or never send
to the exact scorer. No body-read or top-k improvement is inferred.

For the conservative G=4 modal auxiliary-entry proxy, no interval-tight call
also has R+d+J<G. This is strongly affected by G being only four and charging
defaults separately; it is not a byte lower bound. Modal defaults can still
preserve information beyond two-size union in the proxy-small subset: at tau=8,
33 GrQc and 36 Facebook admitted calls do so.

### Bounded G Sensitivity

Pairing, source IDs, union intersections and positive-call denominators are
unchanged. Only feature groups change. Quantiles are empirical nearest-rank
quantiles; "mid / p90 / max" describes the median, 90th percentile and maximum.
r is across all full pairs; s is weighted by positive nonself calls.

| Dataset | G | r mid / p90 / max | Mean core K | Mean owner R | s mid / p90 / max |
| --- | ---: | --- | ---: | ---: | --- |
| GrQc | 4 | 0 / 1 / 3 | 2.320 | 1.776 | 1 / 4 / 4 |
| GrQc | 16 | 1 / 3 / 7 | 4.567 | 3.527 | 1 / 6 / 16 |
| GrQc | 64 | 1 / 4 / 17 | 6.372 | 5.103 | 1 / 8 / 41 |
| Facebook | 4 | 0 / 1 / 3 | 1.801 | 1.463 | 1 / 4 / 4 |
| Facebook | 16 | 1 / 5 / 10 | 5.888 | 4.407 | 2 / 14 / 16 |
| Facebook | 64 | 5 / 13 / 28 | 18.577 | 14.300 | 2 / 25 / 64 |

All-pair means below are static, not query weighted. Admission counts use the
same 1407/12706 positive calls. The active fraction is conditional on admission.
No score is evaluated at G=16 or G=64.

| Dataset | G | tau | Mean e | Mean d | Admitted / refused | Active / admitted |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| GrQc | 16 | 1 | 15.076 | 4.455 | 1205 / 202 | 963/1205 (79.9%) |
| GrQc | 16 | 2 | 15.940 | 4.998 | 1278 / 129 | 1036/1278 (81.1%) |
| GrQc | 16 | 4 | 15.999 | 5.261 | 1355 / 52 | 1113/1355 (82.1%) |
| GrQc | 16 | 8 | 16.000 | 5.304 | 1407 / 0 | 1165/1407 (82.8%) |
| GrQc | 64 | 1 | 63.631 | 8.654 | 1265 / 142 | 1197/1265 (94.6%) |
| GrQc | 64 | 2 | 63.996 | 8.785 | 1392 / 15 | 1324/1392 (95.1%) |
| GrQc | 64 | 4 | 64.000 | 8.790 | 1407 / 0 | 1339/1407 (95.2%) |
| GrQc | 64 | 8 | 64.000 | 8.790 | 1407 / 0 | 1339/1407 (95.2%) |
| Facebook | 16 | 1 | 8.558 | 4.032 | 9105 / 3601 | 5215/9105 (57.3%) |
| Facebook | 16 | 2 | 12.587 | 5.957 | 10493 / 2213 | 6270/10493 (59.8%) |
| Facebook | 16 | 4 | 15.168 | 7.690 | 11390 / 1316 | 7078/11390 (62.1%) |
| Facebook | 16 | 8 | 15.913 | 8.821 | 12083 / 623 | 7759/12083 (64.2%) |
| Facebook | 64 | 1 | 55.927 | 19.182 | 10268 / 2438 | 9179/10268 (89.4%) |
| Facebook | 64 | 2 | 62.941 | 24.469 | 11443 / 1263 | 10349/11443 (90.4%) |
| Facebook | 64 | 4 | 63.985 | 27.561 | 12209 / 497 | 11115/12209 (91.0%) |
| Facebook | 64 | 8 | 64.000 | 27.847 | 12697 / 9 | 11603/12697 (91.4%) |

At G=64,tau=8, e=64 for **every** full pair in both datasets. Fixed-default
exceptions therefore fail to compress this leaf table, even before owner runs
are added. Modal signatures change that conclusion: d averages 23039/2621
(8.790) in GrQc and 56224/2019 (27.847) in Facebook. This is not solely an
unqueried-block effect: among distinct queried pairs the means are 10557/819
(12.890) and 53863/1889 (28.514); among admitted positive calls they are
21637/1407 (15.378) and 365465/12697 (28.784).

The fully charged proxy R+d+J<64 holds for 2613/2621 GrQc and 1353/2019 Facebook
full pairs. On the admitted positive calls it holds for 1389/1407 and
7935/12697; 1325 and 7630 of those calls are active. This is concrete evidence
for investigating modal storage, not evidence of exact-score improvement there.

The larger trees also change r and s. GrQc's maximum r rises to 17; Facebook's
r median/p90/max is 5/13/28 at G=64, so an uncapped 2^r orientation path cannot
be justified by the small-r G=4 receipt. Sparse queries remain common, but
Facebook s reaches 64 and GrQc s reaches 41. The report neither evaluates nor
extrapolates the high-r exact path.

## Bounded Score Check

At G=4, r<=3. Score each positive nonself call admitted at the largest tau once,
then reuse its result for every admitting tau. The predeclared caps are eight
orientations, 136 old evaluator node-visit reservation units per call, and at
most 15,000 exact calls across both datasets. The scoring stage is enabled
only at G=4 and enforces its caps; the two larger-G sensitivity profiles bypass
it entirely. This is the old theorem evaluator applied to original supplied
metadata, not a test of a new compiler.

Compare exact rational scores with three clearly distinguished controls:

1. The existing union filter optimizes over any row size between ell and u.
2. A stronger union-only control uses the two actual endpoint row sizes ell,u.
3. The existing interval-capacity filter also uses per-node count intervals.

Check `actual pair score <= exact <= interval <= two-size union <= old union`.
All actual-pair scores here exclude self by construction. For inactive admitted
calls, additionally check equality to both union controls. No tightened bound is
called a pruned block: the diagnostic does not reproduce the witness heap,
earlier-filter failures, access scheduling, body-read savings, or full outputs.

## Modal Math Check

Assume integer 0<=l<=C, H=l+C, fixed integer tau>=0, and unchanged core
capacities, topology and component L,U. Define sigma=(min(l,tau),min(C,tau)).
For 0<=q<=tau, both `min(q,l)` and `min(q,C)` are unchanged by clipping. Also
`q<=H` iff `q<=sum(sigma)`: clipping cannot change an insufficient sum, since
if either original coordinate reaches tau the clipped sum already admits q.
Thus the original validity decision and both reward contributions are exact.
Keep original component L and U, including nonquery occupancy; replacing these
hard constraints with sums of clipped coordinates would not be justified.

Given owner lookup, a component default plus deviations recovers every leaf's
sigma. Consequently it preserves A=L-sum(min(q,l)) and B=sum(min(q,C)), hence
the same old envelope for admitted queries. An ID outside the original range,
duplicates, invalid ordering, q>tau, q>sum(sigma), or total Q>a must still be
handled by the stated query contract; none is licensed by compression. This
diagnostic uses valid real intersections, not an adversarial API-input test.

In a component with n original leaves, let e_B count l<tau. Every other leaf
has sigma=(tau,tau), so that signature has frequency at least n-e_B. A mode has
at least that frequency, giving d_B=n-mode_frequency<=e_B. Sum over components
to get d<=e. Ties do not affect d; a deterministic lexicographic tie break is
available. Components with no original leaves need no queried default. Empty
original leaves, however, have sigma=(0,0) and must participate in the counts.
Repeated component ownership across holes does not change its histogram or the
owner-run lookup. With O(r+1) core/run/default entries the retained record bound
is O(r+d+1), not an assertion about bytes or resident process memory.

**d=0 does not imply floor collapse.** For a four-leaf example take H=(6,6,4,4),
leaf C=(4,4,3,3), the last pair's parent C=4, the first pair's parent C=8,
root C=12, and ell=12. Feasible row leaf counts are (4,4,3,1) and (4,4,1,3).
There is one strict node. At tau=2, the ordinary component has default (2,2),
and each exceptional singleton component has default (1,2), so d=0. For a=4
and q=(0,0,2,2), attainment and parent C=4 force exceptional row counts (3,1)
or (1,3). The exact maximum overlap is 3, giving 3/13 rather than the union/length
4/12. The leafwise query label placements are feasible:
each exceptional union has four elements partitioned 3+1. This is a mathematical
witness, not a generated workload or a new public-data run.

**The signature is the coarsest local behavior equivalence for this oracle.**
Observe validity and the ordered reward pair for all q=0,...,tau. Let m be the
maximum valid q. If m<tau, then m=H and the rewards at m recover the full l,C.
If m=tau, the rewards at tau recover exactly sigma. Therefore identical behavior
forces identical signatures; the preceding identities prove the converse.
For tau=0 every signature is (0,0). This is local minimality for that specified
oracle, not a bit/word lower bound for an entire core or for Jaccard alone.

The retained independent finite check covers C=0,...,10, l=0,...,C and
tau=0,...,8: **594 local profiles, 2970 query points, and 19899 unordered
profile-pair comparisons including the diagonal**, with no failed assertion.
These counts are separate from the lead's reported tests and do not audit them.
Deterministic compilation can sort component/signature records in O(G log G)
comparisons using O(G) build space. An unqualified worst-case hash-linear claim
is not made. Source graph residence and feature-to-count work remain paid.

Modal/default coding is established machinery. Baunsgaard and Boehm's
[AWARE (2023), Section 3.1, Sparse Dictionary Coding](https://baunsgaard.github.io/assets/pdf/AWARE.pdf)
stores a most-frequent tuple as a default and represents positions of nondefault
tuples. That is the relevant compression precedent here, not a new codec claim.
Ordinary saturation/min identities and interval run-length encoding likewise
remain ordinary arithmetic and representation techniques. The specific local
oracle proof above is the scope of this mathematical check.

## Retained Diagnostic

Run from the repository root with Python 3.11 and `-B`. The AST loader executes
only named pure functions from inspected old helper files, avoiding module-level
benchmark imports and file-building entry points. The exact dependency list and
SHA-256 values are emitted. It is intentionally not a self-contained theorem
checker: it depends on those old metadata/scoring helpers and local data.

The normalized source graph is resident, as are sorted row references, receipts,
small histograms, and one block union. Source-to-count intersections are actually
computed and are part of the diagnostic work, not claimed free production input.
No inference about process peak memory or end-to-end resource savings follows.
The diagnostic computes modal frequencies with temporary `Counter` objects;
that is not an audit or resource model of the lead's deterministic-sort compiler.
Only one dataset/G profile and its per-block state are needed at a time.

```python
import ast
import gzip
import hashlib
import json
from collections import Counter
from fractions import Fraction
from pathlib import Path

ROOT = Path("research_algorithms_20260920")
EXPERIMENTS = ROOT / "experiments"
TAUS = (1, 2, 4, 8)
SCORE_CAP = 15000
score_calls = 0
dependencies = {}


def load_verified_pure_functions(filename, names, namespace):
    path = EXPERIMENTS / filename
    source = path.read_bytes()
    dependencies[str(path)] = hashlib.sha256(source).hexdigest()
    tree = ast.parse(source, filename=str(path))
    definitions = [node for node in tree.body
                   if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in definitions} == set(names)
    exec(compile(ast.Module(body=definitions, type_ignores=[]), str(path), "exec"), namespace)


helpers = {"Fraction": Fraction}
load_verified_pure_functions("bench_bounded_seed_similarity.py",
                             ("read_undirected_graph_source",), helpers)
load_verified_pure_functions("probe_overlap_frontier_similarity.py",
                             ("validate_overlap_metadata_inputs",), helpers)
load_verified_pure_functions("probe_orientation_parameter_similarity.py",
                             ("compute_orientation_capacity_bound",), helpers)
load_verified_pure_functions("probe_laminar_capacity_similarity.py",
                             ("compute_laminar_jaccard_bound",), helpers)
load_verified_pure_functions("probe_selective_capacity_similarity.py",
                             ("compute_interval_capacity_bound",), helpers)


def compute_fraction_overlap_score(q, n, a):
    return Fraction(q, a + n - q) if q else Fraction(0)


def build_existing_pair_metadata(rows, groups):
    union = set().union(*(values for node, values in rows))
    h = [0] * groups
    cap = [0] * (2 * groups)
    for feature in union:
        h[feature % groups] += 1
    for node, values in rows:
        occupancy = [0] * (2 * groups)
        for feature in values:
            occupancy[groups + feature % groups] += 1
        for v in range(groups - 1, 0, -1):
            occupancy[v] = occupancy[2 * v] + occupancy[2 * v + 1]
        cap = [max(a, b) for a, b in zip(cap, occupancy)]
    strict = {v for v in range(1, groups) if cap[2 * v] + cap[2 * v + 1] > cap[v]}
    owners, components = {}, set()

    def assign_original_component_owners(v, inherited):
        if v in strict:
            assign_original_component_owners(2 * v, None)
            assign_original_component_owners(2 * v + 1, None)
            return
        owner = v if inherited is None else inherited
        components.add(owner)
        if v >= groups:
            owners[v - groups] = owner
        else:
            assign_original_component_owners(2 * v, owner)
            assign_original_component_owners(2 * v + 1, owner)

    assign_original_component_owners(1, None)
    r = len(strict)
    runs = 1 + sum(owners[j - 1] != owners[j] for j in range(1, groups))
    k = r + len(components)
    assert runs <= 3 * r + 1 and k <= 3 * r + 1
    lower = [h[j] - cap[groups + j] for j in range(groups)]
    assert all(0 <= lower[j] <= cap[groups + j] for j in range(groups))
    minimum = min(len(values) for node, values in rows)
    assert cap[1] == max(len(values) for node, values in rows)
    modal = {}
    for tau in TAUS:
        frequencies = {}
        for j in range(groups):
            signature = (min(lower[j], tau), min(cap[groups + j], tau))
            frequencies.setdefault(owners[j], Counter())[signature] += 1
        deviations = sum(sum(counts.values()) - max(counts.values())
                         for counts in frequencies.values())
        exceptions = sum(value < tau for value in lower)
        assert deviations <= exceptions
        modal[tau] = (exceptions, deviations, len(frequencies))
    return union, h, cap, lower, minimum, r, k, runs, modal


def create_profile_static_counters():
    return {"n": 0, "r": Counter(), "K": Counter(), "R": Counter(),
            "tau": {tau: {"e": Counter(), "d": Counter(), "e_sum": 0, "d_sum": 0,
                           "aux_less_G": 0, "modal_aux_less_G": 0,
                           "defaults_sum": 0} for tau in TAUS}}


def update_profile_static_counters(target, r, k, runs, modal, groups):
    target["n"] += 1
    for key, value in (("r", r), ("K", k), ("R", runs)):
        target[key][value] += 1
    for tau in TAUS:
        e, d, defaults = modal[tau]
        row = target["tau"][tau]
        row["e"][e] += 1
        row["d"][d] += 1
        row["e_sum"] += e
        row["d_sum"] += d
        row["defaults_sum"] += defaults
        row["aux_less_G"] += runs + e < groups
        row["modal_aux_less_G"] += runs + d + defaults < groups


def create_profile_query_counters():
    return Counter(admitted=0, refused=0, active=0, inactive=0, e_sum=0, d_sum=0,
                   aux_less_G=0, active_aux_less_G=0, tighter_old_union=0,
                   tighter_two_size_union=0, tighter_interval=0,
                   active_tighter_two_size_union=0, modal_aux_less_G=0,
                   active_modal_aux_less_G=0, tighter_interval_aux_less_G=0,
                   tighter_interval_modal_aux_less_G=0,
                   tighter_two_size_modal_aux_less_G=0)


def profile_frozen_source_workload(name, groups):
    global score_calls
    receipt_path = ROOT / f"Similarity-Overlap-{name}-Results.json"
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    assert receipt["groups"] == 4 and receipt["block_size"] == 2
    assert groups in (4, 16, 64)
    scoring = groups == 4
    data_path = EXPERIMENTS / "data" / receipt["dataset_name"]
    assert hashlib.sha256(data_path.read_bytes()).hexdigest() == receipt["sha256"]
    with gzip.open(data_path, "rt", encoding="ascii") as stream:
        graph, source = helpers["read_undirected_graph_source"](stream)
    assert source == receipt["source"]
    selected = receipt["selected_source_ids"]
    assert len(selected) == len(set(selected)) == 30
    rows = sorted(graph.items())
    summaries = {row["node"]: row for row in receipt["summaries"]}
    observed = {sid: Counter() for sid in selected}
    per_source = {sid: {"degree": len(graph[sid]), "positive": 0, "self_positive": 0,
                        "singleton_positive": 0, "tau": {tau: [0, 0] for tau in TAUS}}
                  for sid in selected}
    totals = Counter(all_full_calls=0, zero_full_calls=0, positive_full_calls=0,
                     positive_nonself_calls=0, positive_self_calls=0, self_full_calls=0,
                     singleton_calls=0, singleton_positive_calls=0, singleton_self_calls=0)
    static_all = create_profile_static_counters()
    static_queried = create_profile_static_counters()
    support, weighted_r, query_maximum = Counter(), Counter(), Counter()
    tau_stats = {tau: create_profile_query_counters() for tau in TAUS}
    self_tau = {tau: Counter(admitted=0, refused=0, active=0) for tau in TAUS}
    weighted_e = {tau: Counter() for tau in TAUS}
    weighted_d = {tau: Counter() for tau in TAUS}
    block_histograms = {tau: {key: Counter() for key in ("admitted", "refused", "active")}
                        for tau in TAUS}
    positive_per_block = Counter()
    physical = Counter(full_pairs=0, disjoint_pairs=0, shared_memberships=0, max_shared=0)
    start_scores = score_calls
    for offset in range(0, len(rows), 2):
        pair = rows[offset:offset + 2]
        ids = {node for node, values in pair}
        union = set().union(*(values for node, values in pair))
        calls = []
        for sid in selected:
            overlap = graph[sid] & union
            q = [0] * groups
            for feature in overlap:
                q[feature % groups] += 1
            if overlap:
                observed[sid]["blocks_seen"] += 1
                observed[sid]["block_memberships"] += len(overlap)
            if len(pair) == 1:
                totals["singleton_calls"] += 1
                totals["singleton_self_calls"] += sid in ids
                totals["singleton_positive_calls"] += bool(overlap)
                per_source[sid]["singleton_positive"] += bool(overlap)
            else:
                calls.append((sid, q, sid in ids))
        if len(pair) != 2:
            continue
        shared = len(pair[0][1] & pair[1][1])
        physical["full_pairs"] += 1
        physical["disjoint_pairs"] += shared == 0
        physical["shared_memberships"] += shared
        physical["max_shared"] = max(physical["max_shared"], shared)
        union, h, cap, lower, minimum, r, k, runs, modal = build_existing_pair_metadata(pair, groups)
        assert r <= groups - 1
        update_profile_static_counters(static_all, r, k, runs, modal, groups)
        positives = sum(any(q) and not is_self for sid, q, is_self in calls)
        positive_per_block[positives] += 1
        if positives:
            update_profile_static_counters(static_queried, r, k, runs, modal, groups)
        per_block = {tau: Counter(admitted=0, refused=0, active=0) for tau in TAUS}
        for sid, q, is_self in calls:
            totals["all_full_calls"] += 1
            totals["self_full_calls"] += is_self
            if not any(q):
                totals["zero_full_calls"] += 1
                continue
            totals["positive_full_calls"] += 1
            if is_self:
                totals["positive_self_calls"] += 1
                per_source[sid]["self_positive"] += 1
                for tau in TAUS:
                    admit = max(q) <= tau
                    self_tau[tau]["admitted" if admit else "refused"] += 1
                    self_tau[tau]["active"] += admit and any(q[j] > lower[j] for j in range(groups))
                continue
            totals["positive_nonself_calls"] += 1
            per_source[sid]["positive"] += 1
            support[sum(value > 0 for value in q)] += 1
            weighted_r[r] += 1
            query_maximum[max(q)] += 1
            active = any(q[j] > lower[j] for j in range(groups))
            if scoring and max(q) <= max(TAUS):
                assert score_calls < SCORE_CAP
                score_calls += 1
                a = len(graph[sid])
                exact = helpers["compute_orientation_capacity_bound"](
                    a, minimum, cap, q, h, branch_cap=8, work_cap=136)
                old_union = helpers["compute_laminar_jaccard_bound"](a, minimum, cap, q, "union")
                two_size = max(compute_fraction_overlap_score(min(sum(q), size), size, a)
                               for size in (minimum, cap[1]))
                interval = helpers["compute_interval_capacity_bound"](a, minimum, cap, q, h, 2)
                actual = max(compute_fraction_overlap_score(len(graph[sid] & values), len(values), a)
                             for node, values in pair)
                assert actual <= exact <= interval <= two_size <= old_union
                if not active:
                    assert exact == two_size == old_union
            for tau in TAUS:
                e, d, defaults = modal[tau]
                weighted_e[tau][e] += 1
                weighted_d[tau][d] += 1
                target = tau_stats[tau]
                if max(q) > tau:
                    target["refused"] += 1
                    per_block[tau]["refused"] += 1
                    continue
                target["admitted"] += 1
                target["active" if active else "inactive"] += 1
                target["e_sum"] += e
                target["d_sum"] += d
                target["aux_less_G"] += runs + e < groups
                target["active_aux_less_G"] += active and runs + e < groups
                compact_modal = runs + d + defaults < groups
                target["modal_aux_less_G"] += compact_modal
                target["active_modal_aux_less_G"] += active and compact_modal
                if scoring:
                    target["tighter_old_union"] += exact < old_union
                    target["tighter_two_size_union"] += exact < two_size
                    target["tighter_interval"] += exact < interval
                    target["active_tighter_two_size_union"] += active and exact < two_size
                    target["tighter_interval_aux_less_G"] += exact < interval and runs + e < groups
                    target["tighter_interval_modal_aux_less_G"] += exact < interval and compact_modal
                    target["tighter_two_size_modal_aux_less_G"] += exact < two_size and compact_modal
                per_block[tau]["admitted"] += 1
                per_block[tau]["active"] += active
                per_source[sid]["tau"][tau][0] += 1
                per_source[sid]["tau"][tau][1] += active
        for tau in TAUS:
            for key in block_histograms[tau]:
                block_histograms[tau][key][per_block[tau][key]] += 1
    for key, value in physical.items():
        assert value == receipt["physical_pair_profile"][key]
    for sid in selected:
        assert len(graph[sid]) == summaries[sid]["degree"]
        for key in ("blocks_seen", "block_memberships"):
            assert observed[sid][key] == summaries[sid]["union"]["metrics"].get(key, 0)
        assert len(graph[sid]) <= receipt["query_admission"]["selective_source_cap"]
        assert summaries[sid]["union"]["metrics"]["merge_streams"] <= receipt["query_admission"]["merge_cap"]
    assert totals["all_full_calls"] == 30 * static_all["n"]
    assert totals["all_full_calls"] == totals["zero_full_calls"] + totals["positive_full_calls"]
    assert totals["positive_full_calls"] == totals["positive_nonself_calls"] + totals["positive_self_calls"]
    source_rows = []
    for sid in selected:
        row = per_source[sid]
        source_rows.append([sid, row["degree"], row["positive"], row["self_positive"],
                            row["singleton_positive"]] + [row["tau"][tau] for tau in TAUS])
    tau_output = {}
    for tau in TAUS:
        counts = tau_stats[tau]
        assert counts["admitted"] + counts["refused"] == totals["positive_nonself_calls"]
        assert counts["active"] + counts["inactive"] == counts["admitted"]
        assert counts["active_tighter_two_size_union"] == counts["tighter_two_size_union"]
        for key, histogram in block_histograms[tau].items():
            assert sum(histogram.values()) == static_all["n"]
            assert sum(count * blocks for count, blocks in histogram.items()) == counts[key]
        counts["source_ids_with_admitted"] = sum(per_source[sid]["tau"][tau][0] > 0 for sid in selected)
        counts["source_ids_with_active"] = sum(per_source[sid]["tau"][tau][1] > 0 for sid in selected)
        if not scoring:
            for key in list(counts):
                if "tighter" in key:
                    del counts[key]
        tau_output[tau] = {"calls": counts, "positive_self_calls": self_tau[tau],
                           "weighted_e": weighted_e[tau], "weighted_d": weighted_d[tau],
                           "per_block_histograms": block_histograms[tau]}
    return {"name": name, "data_path": str(data_path), "data_sha256": receipt["sha256"],
            "receipt_path": str(receipt_path), "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
            "normalization": source, "G": groups, "scoring": scoring,
            "physical": physical, "calls": totals,
            "all_full_pairs": static_all, "queried_full_pairs": static_queried,
            "nonself_positive_s": support, "nonself_positive_r": weighted_r,
            "nonself_positive_qmax": query_maximum, "positive_calls_per_block": positive_per_block,
            "tau": tau_output, "source_columns": ["ID", "degree", "positive_nonself", "positive_self",
                "positive_singleton", "tau1_[admitted,active]", "tau2_[admitted,active]",
                "tau4_[admitted,active]", "tau8_[admitted,active]"],
            "sources": source_rows, "unique_exact_calls": score_calls - start_scores,
            "reproduced_posting_totals": {key: sum(observed[sid][key] for sid in selected)
                                           for key in ("blocks_seen", "block_memberships")}}


def check_saturated_signature_equivalence():
    profiles = [(lower, cap) for cap in range(11) for lower in range(cap + 1)]
    points = pairs = 0
    for tau in range(9):
        signatures, behaviors = [], []
        for lower, cap in profiles:
            signature = (min(lower, tau), min(cap, tau))
            behavior = []
            for q in range(tau + 1):
                valid = q <= lower + cap
                assert valid == (q <= sum(signature))
                value = (min(q, lower), min(q, cap)) if valid else None
                clipped = (min(q, signature[0]), min(q, signature[1])) if valid else None
                assert value == clipped
                behavior.append(value)
                points += 1
            signatures.append(signature)
            behaviors.append(tuple(behavior))
        for i in range(len(profiles)):
            for j in range(i, len(profiles)):
                assert (signatures[i] == signatures[j]) == (behaviors[i] == behaviors[j])
                pairs += 1
    return {"local_profiles": len(profiles) * 9, "query_points": points,
            "unordered_pairs_with_diagonal": pairs}


for dataset in ("GrQc", "Facebook"):
    for groups in (4, 16, 64):
        result = profile_frozen_source_workload(dataset, groups)
        if groups != 4:
            for key in ("sources", "source_columns", "nonself_positive_qmax"):
                del result[key]
            for row in result["tau"].values():
                for key in ("weighted_e", "weighted_d", "per_block_histograms"):
                    del row[key]
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
for path, digest in dependencies.items():
    assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest
print(json.dumps({"old_helper_sha256": dependencies, "total_unique_exact_calls": score_calls,
                  "signature_equivalence": check_saturated_signature_equivalence()}, sort_keys=True))
```

Reproduction (reads only; output goes to stdout):

```sh
sed -n '/^```python$/,/^```$/p' research_algorithms_20260920/Similarity-Exception-Workload-Profile.md | sed '1d;$d' | /Users/amuldotexe/.local/bin/python3.11 -B
```

## Retained Stdout

The final retained run exited successfully with all assertions passing. The
following is its complete seven-line JSON output, without command-runner
metadata. Keys of histogram objects are integer values encoded as JSON strings.
All sensitivity score fields are omitted, not zero-score observations.

```jsonl
{"G":4,"all_full_pairs":{"K":{"1":1412,"3":439,"4":610,"5":104,"7":56},"R":{"1":1412,"2":439,"3":714,"4":56},"n":2621,"r":{"0":1412,"1":1049,"2":153,"3":7},"tau":{"1":{"aux_less_G":706,"d":{"0":448,"1":1121,"2":1052},"d_sum":3225,"defaults_sum":4656,"e":{"0":123,"1":242,"2":598,"3":1110,"4":548},"e_sum":6960,"modal_aux_less_G":611},"2":{"aux_less_G":108,"d":{"0":262,"1":970,"2":1212,"3":177},"d_sum":3925,"defaults_sum":4656,"e":{"0":23,"1":49,"2":91,"3":390,"4":2068},"e_sum":9673,"modal_aux_less_G":350},"4":{"aux_less_G":16,"d":{"0":195,"1":860,"2":1158,"3":408},"d_sum":4400,"defaults_sum":4656,"e":{"0":2,"1":4,"2":14,"3":34,"4":2567},"e_sum":10402,"modal_aux_less_G":191},"8":{"aux_less_G":0,"d":{"0":187,"1":810,"2":1088,"3":536},"d_sum":4594,"defaults_sum":4656,"e":{"2":1,"3":2,"4":2618},"e_sum":10480,"modal_aux_less_G":135}}},"calls":{"all_full_calls":78630,"positive_full_calls":1436,"positive_nonself_calls":1407,"positive_self_calls":29,"self_full_calls":30,"singleton_calls":0,"singleton_positive_calls":0,"singleton_self_calls":0,"zero_full_calls":77194},"data_path":"research_algorithms_20260920/experiments/data/ca-GrQc.txt.gz","data_sha256":"a254442cdf5d684712578b630c2e0d7543518ab154ef2341cabb607572ce7230","name":"GrQc","nonself_positive_qmax":{"1":1103,"2":122,"3":34,"4":25,"5":2,"6":27,"7":1,"9":2,"10":1,"12":3,"13":22,"14":50,"15":10,"16":1,"17":1,"18":2,"20":1},"nonself_positive_r":{"0":993,"1":341,"2":70,"3":3},"nonself_positive_s":{"1":881,"2":270,"3":94,"4":162},"normalization":{"duplicate_rows":14484,"edges":14484,"memberships":28968,"nodes":5242,"raw_rows":28980,"self_loop_rows":12},"physical":{"disjoint_pairs":2564,"full_pairs":2621,"max_shared":33,"shared_memberships":160},"positive_calls_per_block":{"0":1802,"1":469,"2":195,"3":87,"4":54,"5":13,"6":1},"queried_full_pairs":{"K":{"1":539,"3":92,"4":146,"5":29,"7":13},"R":{"1":539,"2":92,"3":175,"4":13},"n":819,"r":{"0":539,"1":238,"2":40,"3":2},"tau":{"1":{"aux_less_G":365,"d":{"0":175,"1":330,"2":314},"d_sum":958,"defaults_sum":1300,"e":{"0":84,"1":119,"2":267,"3":273,"4":76},"e_sum":1776,"modal_aux_less_G":270},"2":{"aux_less_G":70,"d":{"0":84,"1":295,"2":379,"3":61},"d_sum":1236,"defaults_sum":1300,"e":{"0":17,"1":40,"2":49,"3":166,"4":547},"e_sum":2824,"modal_aux_less_G":164},"4":{"aux_less_G":13,"d":{"0":46,"1":237,"2":375,"3":161},"d_sum":1470,"defaults_sum":1300,"e":{"0":2,"1":3,"2":10,"3":24,"4":780},"e_sum":3215,"modal_aux_less_G":78},"8":{"aux_less_G":0,"d":{"0":42,"1":207,"2":333,"3":237},"d_sum":1584,"defaults_sum":1300,"e":{"2":1,"3":1,"4":817},"e_sum":3273,"modal_aux_less_G":46}}},"receipt_path":"research_algorithms_20260920/Similarity-Overlap-GrQc-Results.json","receipt_sha256":"f06dc6809191194981fae1f2d7e049183470df2edabe2f24077c65d872653e72","reproduced_posting_totals":{"block_memberships":7237,"blocks_seen":1436},"scoring":true,"source_columns":["ID","degree","positive_nonself","positive_self","positive_singleton","tau1_[admitted,active]","tau2_[admitted,active]","tau4_[admitted,active]","tau8_[admitted,active]"],"sources":[[12295,0,0,0,0,[0,0],[0,0],[0,0],[0,0]],[2307,2,43,1,0,[43,21],[43,21],[43,21],[43,21]],[10910,3,69,1,0,[69,26],[69,26],[69,26],[69,26]],[5862,6,25,1,0,[22,9],[25,11],[25,11],[25,11]],[21281,79,295,1,0,[184,89],[224,119],[249,139],[250,140]],[21012,81,339,1,0,[223,107],[265,141],[285,159],[291,165]],[2117,1,0,1,0,[0,0],[0,0],[0,0],[0,0]],[14808,23,25,1,0,[2,1],[2,1],[2,1],[25,24]],[16958,1,17,1,0,[17,6],[17,6],[17,6],[17,6]],[727,6,47,1,0,[44,20],[47,22],[47,22],[47,22]],[1958,19,95,1,0,[71,26],[89,40],[95,45],[95,45]],[25996,5,41,1,0,[39,14],[41,16],[41,16],[41,16]],[1599,1,5,1,0,[5,3],[5,3],[5,3],[5,3]],[12213,6,6,1,0,[0,0],[0,0],[6,4],[6,4]],[4700,2,24,1,0,[24,9],[24,9],[24,9],[24,9]],[2622,1,4,1,0,[4,2],[4,2],[4,2],[4,2]],[2055,2,85,1,0,[85,50],[85,50],[85,50],[85,50]],[24332,3,41,1,0,[41,15],[41,15],[41,15],[41,15]],[6448,1,0,1,0,[0,0],[0,0],[0,0],[0,0]],[23858,4,9,1,0,[9,6],[9,6],[9,6],[9,6]],[12248,1,0,1,0,[0,0],[0,0],[0,0],[0,0]],[3730,3,44,1,0,[43,20],[44,21],[44,21],[44,21]],[25662,1,1,1,0,[1,0],[1,0],[1,0],[1,0]],[12968,10,93,1,0,[88,36],[92,40],[93,41],[93,41]],[7579,2,15,1,0,[15,5],[15,5],[15,5],[15,5]],[3718,2,16,1,0,[16,12],[16,12],[16,12],[16,12]],[21650,7,35,1,0,[30,8],[35,13],[35,13],[35,13]],[22233,7,14,1,0,[9,7],[13,10],[14,10],[14,10]],[17382,3,15,1,0,[15,8],[15,8],[15,8],[15,8]],[19350,3,4,1,0,[4,3],[4,3],[4,3],[4,3]]],"tau":{"1":{"calls":{"active":503,"active_aux_less_G":153,"active_modal_aux_less_G":167,"active_tighter_two_size_union":354,"admitted":1103,"aux_less_G":518,"d_sum":1258,"e_sum":2317,"inactive":600,"modal_aux_less_G":401,"refused":304,"source_ids_with_active":24,"source_ids_with_admitted":25,"tighter_interval":49,"tighter_interval_aux_less_G":1,"tighter_interval_modal_aux_less_G":0,"tighter_old_union":415,"tighter_two_size_modal_aux_less_G":130,"tighter_two_size_union":354},"per_block_histograms":{"active":{"0":2248,"1":266,"2":88,"3":17,"4":1,"6":1},"admitted":{"0":1869,"1":479,"2":205,"3":60,"4":7,"6":1},"refused":{"0":2410,"1":120,"2":89,"3":2}},"positive_self_calls":{"active":7,"admitted":15,"refused":14},"weighted_d":{"0":321,"1":578,"2":508},"weighted_e":{"0":181,"1":231,"2":451,"3":446,"4":98}},"2":{"calls":{"active":600,"active_aux_less_G":12,"active_modal_aux_less_G":125,"active_tighter_two_size_union":381,"admitted":1225,"aux_less_G":121,"d_sum":1843,"e_sum":4156,"inactive":625,"modal_aux_less_G":262,"refused":182,"source_ids_with_active":24,"source_ids_with_admitted":25,"tighter_interval":57,"tighter_interval_aux_less_G":0,"tighter_interval_modal_aux_less_G":0,"tighter_old_union":489,"tighter_two_size_modal_aux_less_G":89,"tighter_two_size_union":381},"per_block_histograms":{"active":{"0":2190,"1":294,"2":109,"3":26,"4":1,"6":1},"admitted":{"0":1829,"1":470,"2":226,"3":84,"4":10,"5":1,"6":1},"refused":{"0":2498,"1":64,"2":59}},"positive_self_calls":{"active":10,"admitted":19,"refused":10},"weighted_d":{"0":144,"1":520,"2":648,"3":95},"weighted_e":{"0":38,"1":97,"2":104,"3":294,"4":874}},"4":{"calls":{"active":648,"active_aux_less_G":0,"active_modal_aux_less_G":70,"active_tighter_two_size_union":385,"admitted":1284,"aux_less_G":26,"d_sum":2350,"e_sum":5024,"inactive":636,"modal_aux_less_G":116,"refused":123,"source_ids_with_active":25,"source_ids_with_admitted":26,"tighter_interval":59,"tighter_interval_aux_less_G":0,"tighter_interval_modal_aux_less_G":0,"tighter_old_union":531,"tighter_two_size_modal_aux_less_G":47,"tighter_two_size_union":385},"per_block_histograms":{"active":{"0":2169,"1":297,"2":120,"3":31,"4":3,"6":1},"admitted":{"0":1822,"1":452,"2":232,"3":96,"4":16,"5":2,"6":1},"refused":{"0":2543,"1":33,"2":45}},"positive_self_calls":{"active":13,"admitted":23,"refused":6},"weighted_d":{"0":65,"1":407,"2":663,"3":272},"weighted_e":{"0":3,"1":9,"2":28,"3":51,"4":1316}},"8":{"calls":{"active":678,"active_aux_less_G":0,"active_modal_aux_less_G":47,"active_tighter_two_size_union":385,"admitted":1314,"aux_less_G":0,"d_sum":2603,"e_sum":5249,"inactive":636,"modal_aux_less_G":70,"refused":93,"source_ids_with_active":25,"source_ids_with_admitted":26,"tighter_interval":59,"tighter_interval_aux_less_G":0,"tighter_interval_modal_aux_less_G":0,"tighter_old_union":559,"tighter_two_size_modal_aux_less_G":33,"tighter_two_size_union":385},"per_block_histograms":{"active":{"0":2146,"1":318,"2":119,"3":32,"4":5,"6":1},"admitted":{"0":1802,"1":469,"2":232,"3":95,"4":20,"5":2,"6":1},"refused":{"0":2572,"1":5,"2":44}},"positive_self_calls":{"active":17,"admitted":27,"refused":2},"weighted_d":{"0":60,"1":345,"2":582,"3":420},"weighted_e":{"2":2,"3":5,"4":1400}}},"unique_exact_calls":1314}
{"G":16,"all_full_pairs":{"K":{"1":482,"3":254,"4":1027,"5":48,"6":184,"7":357,"8":25,"9":77,"10":88,"11":18,"12":24,"13":22,"14":7,"15":5,"16":1,"18":1,"22":1},"R":{"1":482,"2":254,"3":680,"4":566,"5":259,"6":178,"7":98,"8":62,"9":25,"10":11,"11":4,"12":1,"14":1},"n":2621,"r":{"0":482,"1":1281,"2":580,"3":191,"4":71,"5":13,"6":2,"7":1},"tau":{"1":{"aux_less_G":230,"d":{"0":17,"1":79,"2":290,"3":478,"4":504,"5":498,"6":370,"7":254,"8":106,"9":19,"10":6},"d_sum":11676,"defaults_sum":8519,"e":{"2":1,"4":2,"5":2,"6":4,"7":7,"8":5,"9":19,"10":15,"11":27,"12":52,"13":105,"14":278,"15":781,"16":1323},"e_sum":39515,"modal_aux_less_G":2232},"2":{"aux_less_G":8,"d":{"0":13,"1":69,"2":247,"3":437,"4":450,"5":407,"6":338,"7":252,"8":200,"9":111,"10":68,"11":28,"12":1},"d_sum":13100,"defaults_sum":8519,"e":{"8":1,"10":1,"11":1,"12":5,"13":3,"14":15,"15":78,"16":2517},"e_sum":41780,"modal_aux_less_G":2162},"4":{"aux_less_G":0,"d":{"0":13,"1":66,"2":243,"3":432,"4":431,"5":390,"6":304,"7":222,"8":189,"9":109,"10":98,"11":80,"12":38,"13":5,"14":1},"d_sum":13788,"defaults_sum":8519,"e":{"15":2,"16":2619},"e_sum":41934,"modal_aux_less_G":2116},"8":{"aux_less_G":0,"d":{"0":13,"1":66,"2":243,"3":432,"4":430,"5":386,"6":304,"7":221,"8":183,"9":103,"10":92,"11":76,"12":51,"13":20,"14":1},"d_sum":13902,"defaults_sum":8519,"e":{"16":2621},"e_sum":41936,"modal_aux_less_G":2113}}},"calls":{"all_full_calls":78630,"positive_full_calls":1436,"positive_nonself_calls":1407,"positive_self_calls":29,"self_full_calls":30,"singleton_calls":0,"singleton_positive_calls":0,"singleton_self_calls":0,"zero_full_calls":77194},"data_path":"research_algorithms_20260920/experiments/data/ca-GrQc.txt.gz","data_sha256":"a254442cdf5d684712578b630c2e0d7543518ab154ef2341cabb607572ce7230","name":"GrQc","nonself_positive_r":{"0":444,"1":493,"2":265,"3":127,"4":59,"5":15,"6":2,"7":2},"nonself_positive_s":{"1":847,"2":250,"3":99,"4":33,"5":17,"6":30,"7":7,"8":4,"10":3,"11":7,"12":20,"14":7,"15":76,"16":7},"normalization":{"duplicate_rows":14484,"edges":14484,"memberships":28968,"nodes":5242,"raw_rows":28980,"self_loop_rows":12},"physical":{"disjoint_pairs":2564,"full_pairs":2621,"max_shared":33,"shared_memberships":160},"positive_calls_per_block":{"0":1802,"1":469,"2":195,"3":87,"4":54,"5":13,"6":1},"queried_full_pairs":{"K":{"1":212,"3":48,"4":263,"5":6,"6":47,"7":124,"8":12,"9":32,"10":37,"11":11,"12":10,"13":6,"14":6,"15":3,"16":1,"22":1},"R":{"1":212,"2":48,"3":144,"4":168,"5":91,"6":65,"7":41,"8":33,"9":9,"10":6,"11":1,"14":1},"n":819,"r":{"0":212,"1":311,"2":174,"3":82,"4":29,"5":9,"6":1,"7":1},"tau":{"1":{"aux_less_G":141,"d":{"0":5,"1":9,"2":52,"3":101,"4":145,"5":174,"6":160,"7":119,"8":42,"9":10,"10":2},"d_sum":4105,"defaults_sum":2723,"e":{"2":1,"4":1,"5":1,"6":3,"7":6,"8":4,"9":15,"10":13,"11":23,"12":28,"13":56,"14":138,"15":263,"16":267},"e_sum":11834,"modal_aux_less_G":631},"2":{"aux_less_G":7,"d":{"0":2,"1":6,"2":25,"3":74,"4":114,"5":123,"6":139,"7":128,"8":97,"9":69,"10":22,"11":19,"12":1},"d_sum":4917,"defaults_sum":2723,"e":{"8":1,"10":1,"11":1,"12":4,"13":2,"14":11,"15":50,"16":749},"e_sum":12991,"modal_aux_less_G":599},"4":{"aux_less_G":0,"d":{"0":2,"1":3,"2":21,"3":69,"4":100,"5":111,"6":119,"7":108,"8":89,"9":59,"10":48,"11":57,"12":27,"13":5,"14":1},"d_sum":5430,"defaults_sum":2723,"e":{"15":2,"16":817},"e_sum":13102,"modal_aux_less_G":570},"8":{"aux_less_G":0,"d":{"0":2,"1":3,"2":21,"3":69,"4":99,"5":107,"6":119,"7":107,"8":83,"9":53,"10":42,"11":53,"12":40,"13":20,"14":1},"d_sum":5544,"defaults_sum":2723,"e":{"16":819},"e_sum":13104,"modal_aux_less_G":567}}},"receipt_path":"research_algorithms_20260920/Similarity-Overlap-GrQc-Results.json","receipt_sha256":"f06dc6809191194981fae1f2d7e049183470df2edabe2f24077c65d872653e72","reproduced_posting_totals":{"block_memberships":7237,"blocks_seen":1436},"scoring":false,"tau":{"1":{"calls":{"active":963,"active_aux_less_G":148,"active_modal_aux_less_G":746,"admitted":1205,"aux_less_G":240,"d_sum":6093,"e_sum":17199,"inactive":242,"modal_aux_less_G":918,"refused":202,"source_ids_with_active":25,"source_ids_with_admitted":25},"positive_self_calls":{"active":16,"admitted":18,"refused":11}},"2":{"calls":{"active":1036,"active_aux_less_G":6,"active_modal_aux_less_G":748,"admitted":1278,"aux_less_G":10,"d_sum":7877,"e_sum":20248,"inactive":242,"modal_aux_less_G":912,"refused":129,"source_ids_with_active":26,"source_ids_with_admitted":26},"positive_self_calls":{"active":21,"admitted":23,"refused":6}},"4":{"calls":{"active":1113,"active_aux_less_G":0,"active_modal_aux_less_G":772,"admitted":1355,"aux_less_G":0,"d_sum":9529,"e_sum":21675,"inactive":242,"modal_aux_less_G":924,"refused":52,"source_ids_with_active":26,"source_ids_with_admitted":26},"positive_self_calls":{"active":25,"admitted":27,"refused":2}},"8":{"calls":{"active":1165,"active_aux_less_G":0,"active_modal_aux_less_G":806,"admitted":1407,"aux_less_G":0,"d_sum":10429,"e_sum":22512,"inactive":242,"modal_aux_less_G":956,"refused":0,"source_ids_with_active":26,"source_ids_with_admitted":26},"positive_self_calls":{"active":26,"admitted":28,"refused":1}}},"unique_exact_calls":0}
{"G":64,"all_full_pairs":{"K":{"1":123,"3":213,"4":1022,"5":41,"6":149,"7":492,"8":23,"9":75,"10":205,"11":15,"12":46,"13":85,"14":12,"15":16,"16":28,"17":8,"18":11,"19":11,"21":12,"22":8,"23":1,"24":5,"25":3,"26":1,"28":5,"29":2,"30":2,"31":2,"32":1,"34":2,"38":1,"49":1},"R":{"1":123,"2":213,"3":548,"4":645,"5":214,"6":265,"7":196,"8":87,"9":98,"10":65,"11":48,"12":26,"13":30,"14":13,"15":9,"16":8,"17":11,"18":1,"19":5,"20":3,"21":5,"23":3,"24":3,"26":1,"34":1},"n":2621,"r":{"0":123,"1":1235,"2":679,"3":302,"4":148,"5":56,"6":29,"7":22,"8":10,"9":4,"10":6,"11":3,"12":1,"13":2,"17":1},"tau":{"1":{"aux_less_G":19,"d":{"0":2,"1":12,"2":148,"3":312,"4":346,"5":278,"6":244,"7":176,"8":152,"9":148,"10":98,"11":93,"12":65,"13":59,"14":66,"15":58,"16":41,"17":41,"18":41,"19":27,"20":29,"21":18,"22":19,"23":16,"24":9,"25":14,"26":17,"27":15,"28":14,"29":14,"30":17,"31":20,"32":8,"33":4},"d_sum":22683,"defaults_sum":11656,"e":{"54":2,"55":3,"56":3,"57":2,"58":6,"59":4,"60":19,"61":30,"62":107,"63":447,"64":1998},"e_sum":166776,"modal_aux_less_G":2614},"2":{"aux_less_G":0,"d":{"0":2,"1":12,"2":148,"3":312,"4":346,"5":278,"6":244,"7":175,"8":151,"9":150,"10":98,"11":93,"12":64,"13":59,"14":63,"15":56,"16":43,"17":40,"18":42,"19":23,"20":29,"21":18,"22":18,"23":12,"24":10,"25":9,"26":18,"27":16,"28":8,"29":13,"30":14,"31":18,"32":7,"33":9,"34":5,"35":3,"36":4,"37":4,"38":3,"40":2,"41":1,"42":1},"d_sum":23025,"defaults_sum":11656,"e":{"63":11,"64":2610},"e_sum":167733,"modal_aux_less_G":2613},"4":{"aux_less_G":0,"d":{"0":2,"1":12,"2":148,"3":312,"4":346,"5":278,"6":244,"7":175,"8":151,"9":150,"10":98,"11":93,"12":64,"13":59,"14":63,"15":56,"16":43,"17":40,"18":42,"19":23,"20":29,"21":18,"22":18,"23":12,"24":10,"25":9,"26":18,"27":16,"28":8,"29":13,"30":14,"31":18,"32":7,"33":9,"34":5,"35":1,"36":4,"37":4,"38":2,"39":1,"40":2,"41":2,"42":2},"d_sum":23039,"defaults_sum":11656,"e":{"64":2621},"e_sum":167744,"modal_aux_less_G":2613},"8":{"aux_less_G":0,"d":{"0":2,"1":12,"2":148,"3":312,"4":346,"5":278,"6":244,"7":175,"8":151,"9":150,"10":98,"11":93,"12":64,"13":59,"14":63,"15":56,"16":43,"17":40,"18":42,"19":23,"20":29,"21":18,"22":18,"23":12,"24":10,"25":9,"26":18,"27":16,"28":8,"29":13,"30":14,"31":18,"32":7,"33":9,"34":5,"35":1,"36":4,"37":4,"38":2,"39":1,"40":2,"41":2,"42":2},"d_sum":23039,"defaults_sum":11656,"e":{"64":2621},"e_sum":167744,"modal_aux_less_G":2613}}},"calls":{"all_full_calls":78630,"positive_full_calls":1436,"positive_nonself_calls":1407,"positive_self_calls":29,"self_full_calls":30,"singleton_calls":0,"singleton_positive_calls":0,"singleton_self_calls":0,"zero_full_calls":77194},"data_path":"research_algorithms_20260920/experiments/data/ca-GrQc.txt.gz","data_sha256":"a254442cdf5d684712578b630c2e0d7543518ab154ef2341cabb607572ce7230","name":"GrQc","nonself_positive_r":{"0":112,"1":472,"2":335,"3":200,"4":115,"5":56,"6":35,"7":43,"8":18,"9":6,"10":6,"11":3,"12":1,"13":3,"17":2},"nonself_positive_s":{"1":845,"2":250,"3":84,"4":37,"5":16,"6":20,"7":12,"8":9,"9":10,"10":4,"12":1,"15":2,"16":2,"17":2,"19":17,"20":6,"31":33,"32":38,"33":7,"34":5,"35":1,"36":4,"38":1,"41":1},"normalization":{"duplicate_rows":14484,"edges":14484,"memberships":28968,"nodes":5242,"raw_rows":28980,"self_loop_rows":12},"physical":{"disjoint_pairs":2564,"full_pairs":2621,"max_shared":33,"shared_memberships":160},"positive_calls_per_block":{"0":1802,"1":469,"2":195,"3":87,"4":54,"5":13,"6":1},"queried_full_pairs":{"K":{"1":48,"3":20,"4":270,"5":6,"6":30,"7":176,"8":8,"9":24,"10":90,"11":6,"12":21,"13":39,"14":4,"15":10,"16":16,"17":4,"18":9,"19":5,"21":10,"22":6,"24":2,"25":3,"28":3,"29":1,"30":2,"31":2,"34":2,"38":1,"49":1},"R":{"1":48,"2":20,"3":105,"4":203,"5":55,"6":85,"7":88,"8":34,"9":46,"10":31,"11":26,"12":16,"13":20,"14":5,"15":8,"16":6,"17":6,"18":1,"19":2,"20":2,"21":5,"23":3,"24":2,"26":1,"34":1},"n":819,"r":{"0":48,"1":290,"2":211,"3":121,"4":68,"5":30,"6":16,"7":17,"8":6,"9":3,"10":4,"11":1,"12":1,"13":2,"17":1},"tau":{"1":{"aux_less_G":12,"d":{"2":7,"3":33,"4":62,"5":61,"6":71,"7":47,"8":45,"9":63,"10":36,"11":43,"12":26,"13":30,"14":31,"15":20,"16":20,"17":25,"18":20,"19":18,"20":18,"21":16,"22":17,"23":12,"24":6,"25":7,"26":12,"27":5,"28":9,"29":13,"30":15,"31":19,"32":8,"33":4},"d_sum":10232,"defaults_sum":4348,"e":{"54":2,"55":2,"56":3,"57":1,"58":5,"59":3,"60":18,"61":20,"62":64,"63":192,"64":509},"e_sum":51850,"modal_aux_less_G":812},"2":{"aux_less_G":0,"d":{"2":7,"3":33,"4":62,"5":61,"6":71,"7":46,"8":46,"9":63,"10":36,"11":43,"12":25,"13":30,"14":30,"15":21,"16":21,"17":21,"18":23,"19":13,"20":16,"21":16,"22":16,"23":10,"24":7,"25":4,"26":11,"27":5,"28":3,"29":11,"30":12,"31":17,"32":7,"33":9,"34":5,"35":3,"36":4,"37":4,"38":3,"40":2,"41":1,"42":1},"d_sum":10543,"defaults_sum":4348,"e":{"63":8,"64":811},"e_sum":52408,"modal_aux_less_G":811},"4":{"aux_less_G":0,"d":{"2":7,"3":33,"4":62,"5":61,"6":71,"7":46,"8":46,"9":63,"10":36,"11":43,"12":25,"13":30,"14":30,"15":21,"16":21,"17":21,"18":23,"19":13,"20":16,"21":16,"22":16,"23":10,"24":7,"25":4,"26":11,"27":5,"28":3,"29":11,"30":12,"31":17,"32":7,"33":9,"34":5,"35":1,"36":4,"37":4,"38":2,"39":1,"40":2,"41":2,"42":2},"d_sum":10557,"defaults_sum":4348,"e":{"64":819},"e_sum":52416,"modal_aux_less_G":811},"8":{"aux_less_G":0,"d":{"2":7,"3":33,"4":62,"5":61,"6":71,"7":46,"8":46,"9":63,"10":36,"11":43,"12":25,"13":30,"14":30,"15":21,"16":21,"17":21,"18":23,"19":13,"20":16,"21":16,"22":16,"23":10,"24":7,"25":4,"26":11,"27":5,"28":3,"29":11,"30":12,"31":17,"32":7,"33":9,"34":5,"35":1,"36":4,"37":4,"38":2,"39":1,"40":2,"41":2,"42":2},"d_sum":10557,"defaults_sum":4348,"e":{"64":819},"e_sum":52416,"modal_aux_less_G":811}}},"receipt_path":"research_algorithms_20260920/Similarity-Overlap-GrQc-Results.json","receipt_sha256":"f06dc6809191194981fae1f2d7e049183470df2edabe2f24077c65d872653e72","reproduced_posting_totals":{"block_memberships":7237,"blocks_seen":1436},"scoring":false,"tau":{"1":{"calls":{"active":1197,"active_aux_less_G":22,"active_modal_aux_less_G":1189,"admitted":1265,"aux_less_G":25,"d_sum":16895,"e_sum":79948,"inactive":68,"modal_aux_less_G":1254,"refused":142,"source_ids_with_active":26,"source_ids_with_admitted":26},"positive_self_calls":{"active":20,"admitted":22,"refused":7}},"2":{"calls":{"active":1324,"active_aux_less_G":0,"active_modal_aux_less_G":1310,"admitted":1392,"aux_less_G":0,"d_sum":21057,"e_sum":89068,"inactive":68,"modal_aux_less_G":1374,"refused":15,"source_ids_with_active":26,"source_ids_with_admitted":26},"positive_self_calls":{"active":25,"admitted":27,"refused":2}},"4":{"calls":{"active":1339,"active_aux_less_G":0,"active_modal_aux_less_G":1325,"admitted":1407,"aux_less_G":0,"d_sum":21637,"e_sum":90048,"inactive":68,"modal_aux_less_G":1389,"refused":0,"source_ids_with_active":26,"source_ids_with_admitted":26},"positive_self_calls":{"active":27,"admitted":29,"refused":0}},"8":{"calls":{"active":1339,"active_aux_less_G":0,"active_modal_aux_less_G":1325,"admitted":1407,"aux_less_G":0,"d_sum":21637,"e_sum":90048,"inactive":68,"modal_aux_less_G":1389,"refused":0,"source_ids_with_active":26,"source_ids_with_admitted":26},"positive_self_calls":{"active":27,"admitted":29,"refused":0}}},"unique_exact_calls":0}
{"G":4,"all_full_pairs":{"K":{"1":1539,"3":93,"4":251,"5":69,"7":67},"R":{"1":1539,"2":93,"3":320,"4":67},"n":2019,"r":{"0":1539,"1":344,"2":125,"3":11},"tau":{"1":{"aux_less_G":1634,"d":{"0":1438,"1":401,"2":180},"d_sum":761,"defaults_sum":2953,"e":{"0":1313,"1":269,"2":192,"3":148,"4":97},"e_sum":1485,"modal_aux_less_G":1362},"2":{"aux_less_G":1290,"d":{"0":1053,"1":529,"2":423,"3":14},"d_sum":1417,"defaults_sum":2953,"e":{"0":916,"1":292,"2":242,"3":229,"4":340},"e_sum":2823,"modal_aux_less_G":1118},"4":{"aux_less_G":768,"d":{"0":606,"1":533,"2":714,"3":166},"d_sum":2459,"defaults_sum":2953,"e":{"0":480,"1":185,"2":217,"3":266,"4":871},"e_sum":4901,"modal_aux_less_G":706},"8":{"aux_less_G":311,"d":{"0":278,"1":492,"2":744,"3":505},"d_sum":3495,"defaults_sum":2953,"e":{"0":171,"1":93,"2":103,"3":133,"4":1519},"e_sum":6774,"modal_aux_less_G":363}}},"calls":{"all_full_calls":60570,"positive_full_calls":12736,"positive_nonself_calls":12706,"positive_self_calls":30,"self_full_calls":30,"singleton_calls":30,"singleton_positive_calls":0,"singleton_self_calls":0,"zero_full_calls":47834},"data_path":"research_algorithms_20260920/experiments/data/facebook_combined.txt.gz","data_sha256":"125e84db872eeba443d270c70315c256b0af43a502fcfe51f50621166ad035d7","name":"Facebook","nonself_positive_qmax":{"1":7824,"2":1650,"3":598,"4":387,"5":278,"6":190,"7":152,"8":149,"9":109,"10":99,"11":86,"12":90,"13":79,"14":68,"15":37,"16":44,"17":40,"18":35,"19":39,"20":39,"21":34,"22":24,"23":42,"24":30,"25":30,"26":25,"27":28,"28":25,"29":20,"30":18,"31":34,"32":14,"33":18,"34":27,"35":12,"36":29,"37":20,"38":21,"39":30,"40":29,"41":19,"42":30,"43":30,"44":16,"45":9,"46":5,"47":5,"48":8,"49":7,"50":3,"51":6,"52":4,"53":4,"54":3,"55":6,"56":4,"57":6,"58":3,"59":4,"60":1,"61":6,"62":1,"63":1,"64":2,"65":3,"66":1,"67":4,"68":1,"69":2,"70":1,"71":1,"72":1,"73":3,"74":1,"75":2},"nonself_positive_r":{"0":9865,"1":2028,"2":736,"3":77},"nonself_positive_s":{"1":6562,"2":2069,"3":1260,"4":2815},"normalization":{"duplicate_rows":0,"edges":88234,"memberships":176468,"nodes":4039,"raw_rows":88234,"self_loop_rows":0},"physical":{"disjoint_pairs":3,"full_pairs":2019,"max_shared":169,"shared_memberships":12651},"positive_calls_per_block":{"0":130,"2":25,"3":89,"4":268,"5":237,"6":34,"7":527,"8":291,"9":374,"10":16,"11":2,"12":5,"13":15,"14":2,"15":1,"20":2,"25":1},"queried_full_pairs":{"K":{"1":1450,"3":85,"4":228,"5":64,"7":62},"R":{"1":1450,"2":85,"3":292,"4":62},"n":1889,"r":{"0":1450,"1":313,"2":116,"3":10},"tau":{"1":{"aux_less_G":1565,"d":{"0":1374,"1":351,"2":164},"d_sum":679,"defaults_sum":2744,"e":{"0":1272,"1":239,"2":179,"3":121,"4":78},"e_sum":1272,"modal_aux_less_G":1288},"2":{"aux_less_G":1254,"d":{"0":1023,"1":468,"2":386,"3":12},"d_sum":1276,"defaults_sum":2744,"e":{"0":900,"1":271,"2":228,"3":209,"4":281},"e_sum":2478,"modal_aux_less_G":1064},"4":{"aux_less_G":762,"d":{"0":594,"1":489,"2":654,"3":152},"d_sum":2253,"defaults_sum":2744,"e":{"0":480,"1":182,"2":212,"3":248,"4":767},"e_sum":4418,"modal_aux_less_G":685},"8":{"aux_less_G":311,"d":{"0":268,"1":454,"2":694,"3":473},"d_sum":3261,"defaults_sum":2744,"e":{"0":171,"1":93,"2":103,"3":133,"4":1389},"e_sum":6254,"modal_aux_less_G":350}}},"receipt_path":"research_algorithms_20260920/Similarity-Overlap-Facebook-Results.json","receipt_sha256":"2f78c480d64d1c8df4f307bbfe992a13c767a287f634b46c57e9501ee21336ba","reproduced_posting_totals":{"block_memberships":155786,"blocks_seen":12736},"scoring":true,"source_columns":["ID","degree","positive_nonself","positive_self","positive_singleton","tau1_[admitted,active]","tau2_[admitted,active]","tau4_[admitted,active]","tau8_[admitted,active]"],"sources":[[11,1,173,1,0,[173,61],[173,61],[173,61],[173,61]],[2816,11,405,1,0,[264,42],[367,63],[405,72],[405,72]],[2023,25,381,1,0,[226,32],[313,53],[349,60],[380,71]],[1980,57,381,1,0,[296,34],[310,35],[314,37],[325,43]],[1684,792,934,1,0,[491,67],[522,73],[545,87],[593,128]],[107,1045,1422,1,0,[684,147],[819,181],[878,222],[930,269]],[212,18,173,1,0,[69,41],[98,59],[129,80],[169,119]],[3822,30,276,1,0,[183,57],[220,71],[242,84],[269,107]],[1434,15,551,1,0,[342,55],[536,86],[550,94],[551,95]],[3118,44,405,1,0,[166,38],[236,67],[316,98],[399,145]],[1666,36,1065,1,0,[929,129],[1030,150],[1056,156],[1065,160]],[69,10,173,1,0,[98,54],[146,86],[173,109],[173,109]],[191,3,173,1,0,[173,66],[173,66],[173,66],[173,66]],[3522,9,276,1,0,[222,68],[275,89],[276,90],[276,90]],[2599,14,381,1,0,[207,35],[254,40],[336,61],[381,85]],[156,12,277,1,0,[201,58],[251,89],[277,110],[277,110]],[1217,47,613,1,0,[252,67],[411,104],[559,148],[608,172]],[483,231,691,1,0,[214,68],[315,104],[383,134],[486,202]],[273,9,173,1,0,[163,61],[164,62],[173,70],[173,70]],[2806,50,405,1,0,[230,35],[253,40],[297,60],[367,109]],[205,2,173,1,0,[169,61],[173,65],[173,65],[173,65]],[3001,35,434,1,0,[294,43],[360,64],[412,79],[432,91]],[3620,16,276,1,0,[214,61],[231,68],[255,82],[276,97]],[2446,161,381,1,0,[110,22],[169,38],[188,45],[194,48]],[2941,11,417,1,0,[357,50],[411,59],[417,61],[417,61]],[3920,16,276,1,0,[250,67],[256,69],[263,74],[276,86]],[662,24,98,1,0,[76,21],[76,21],[76,21],[83,27]],[2402,15,381,1,0,[310,33],[346,40],[371,47],[381,52]],[1219,54,537,1,0,[244,43],[277,47],[324,65],[418,121]],[2830,22,405,1,0,[217,41],[309,60],[376,84],[405,101]]],"tau":{"1":{"calls":{"active":1657,"active_aux_less_G":664,"active_modal_aux_less_G":901,"active_tighter_two_size_union":40,"admitted":7824,"aux_less_G":6290,"d_sum":3160,"e_sum":6014,"inactive":6167,"modal_aux_less_G":5244,"refused":4882,"source_ids_with_active":30,"source_ids_with_admitted":30,"tighter_interval":2,"tighter_interval_aux_less_G":0,"tighter_interval_modal_aux_less_G":0,"tighter_old_union":88,"tighter_two_size_modal_aux_less_G":20,"tighter_two_size_union":40},"per_block_histograms":{"active":{"0":1607,"1":80,"2":50,"3":36,"4":90,"5":52,"6":24,"7":36,"8":43,"9":1},"admitted":{"0":151,"1":70,"2":279,"3":328,"4":491,"5":317,"6":150,"7":143,"8":75,"9":9,"10":2,"11":1,"13":1,"15":1,"22":1},"refused":{"0":318,"1":331,"2":467,"3":386,"4":254,"5":179,"6":53,"7":21,"8":8,"9":1,"10":1}},"positive_self_calls":{"active":2,"admitted":2,"refused":28},"weighted_d":{"0":9373,"1":2282,"2":1051},"weighted_e":{"0":8768,"1":1564,"2":1127,"3":781,"4":466}},"2":{"calls":{"active":2110,"active_aux_less_G":361,"active_modal_aux_less_G":809,"active_tighter_two_size_union":146,"admitted":9474,"aux_less_G":6129,"d_sum":6694,"e_sum":13061,"inactive":7364,"modal_aux_less_G":5253,"refused":3232,"source_ids_with_active":30,"source_ids_with_admitted":30,"tighter_interval":14,"tighter_interval_aux_less_G":0,"tighter_interval_modal_aux_less_G":0,"tighter_old_union":274,"tighter_two_size_modal_aux_less_G":35,"tighter_two_size_union":146},"per_block_histograms":{"active":{"0":1437,"1":169,"2":85,"3":45,"4":87,"5":64,"6":31,"7":29,"8":69,"9":3},"admitted":{"0":138,"1":36,"2":125,"3":159,"4":488,"5":389,"6":238,"7":253,"8":164,"9":16,"10":6,"11":3,"12":1,"14":1,"17":1,"23":1},"refused":{"0":480,"1":483,"2":607,"3":304,"4":114,"5":22,"6":8,"9":1}},"positive_self_calls":{"active":3,"admitted":3,"refused":27},"weighted_d":{"0":7080,"1":3014,"2":2535,"3":77},"weighted_e":{"0":6330,"1":1793,"2":1479,"3":1336,"4":1768}},"4":{"calls":{"active":2522,"active_aux_less_G":84,"active_modal_aux_less_G":629,"active_tighter_two_size_union":276,"admitted":10459,"aux_less_G":4179,"d_sum":12623,"e_sum":24589,"inactive":7937,"modal_aux_less_G":3750,"refused":2247,"source_ids_with_active":30,"source_ids_with_admitted":30,"tighter_interval":31,"tighter_interval_aux_less_G":0,"tighter_interval_modal_aux_less_G":0,"tighter_old_union":498,"tighter_two_size_modal_aux_less_G":42,"tighter_two_size_union":276},"per_block_histograms":{"active":{"0":1258,"1":258,"2":133,"3":70,"4":90,"5":57,"6":29,"7":35,"8":81,"9":6,"10":1,"12":1},"admitted":{"0":130,"1":13,"2":103,"3":98,"4":420,"5":410,"6":192,"7":278,"8":325,"9":27,"10":9,"11":7,"12":3,"13":1,"14":1,"20":1,"25":1},"refused":{"0":647,"1":672,"2":561,"3":110,"4":25,"5":2,"6":1,"7":1}},"positive_self_calls":{"active":9,"admitted":9,"refused":21},"weighted_d":{"0":4212,"1":3175,"2":4356,"3":963},"weighted_e":{"0":3525,"1":1244,"2":1433,"3":1659,"4":4845}},"8":{"calls":{"active":3032,"active_aux_less_G":22,"active_modal_aux_less_G":478,"active_tighter_two_size_union":423,"admitted":11228,"aux_less_G":1834,"d_sum":19436,"e_sum":37122,"inactive":8196,"modal_aux_less_G":2081,"refused":1478,"source_ids_with_active":30,"source_ids_with_admitted":30,"tighter_interval":46,"tighter_interval_aux_less_G":0,"tighter_interval_modal_aux_less_G":0,"tighter_old_union":831,"tighter_two_size_modal_aux_less_G":36,"tighter_two_size_union":423},"per_block_histograms":{"active":{"0":1012,"1":402,"2":174,"3":105,"4":110,"5":42,"6":37,"7":26,"8":90,"9":19,"10":1,"12":1},"admitted":{"0":130,"1":6,"2":47,"3":96,"4":398,"5":333,"6":253,"7":178,"8":483,"9":67,"10":5,"11":5,"12":10,"13":5,"17":1,"20":1,"25":1},"refused":{"0":875,"1":828,"2":299,"3":16,"4":1}},"positive_self_calls":{"active":15,"admitted":15,"refused":15},"weighted_d":{"0":1884,"1":3011,"2":4731,"3":3080},"weighted_e":{"0":1250,"1":696,"2":751,"3":950,"4":9059}}},"unique_exact_calls":11228}
{"G":16,"all_full_pairs":{"K":{"1":769,"3":9,"4":383,"5":4,"6":42,"7":196,"8":16,"9":86,"10":117,"11":27,"12":62,"13":81,"14":62,"15":39,"16":34,"17":32,"18":14,"19":14,"20":10,"21":11,"22":3,"23":2,"24":3,"25":2,"26":1},"R":{"1":769,"2":9,"3":144,"4":274,"5":91,"6":154,"7":143,"8":113,"9":116,"10":94,"11":56,"12":35,"13":16,"14":5},"n":2019,"r":{"0":769,"1":392,"2":241,"3":213,"4":156,"5":136,"6":59,"7":31,"8":12,"9":6,"10":4},"tau":{"1":{"aux_less_G":1291,"d":{"0":197,"1":170,"2":212,"3":226,"4":288,"5":307,"6":290,"7":209,"8":105,"9":11,"10":4},"d_sum":8141,"defaults_sum":8160,"e":{"0":146,"1":98,"2":98,"3":102,"4":79,"5":100,"6":102,"7":116,"8":119,"9":129,"10":114,"11":119,"12":109,"13":135,"14":134,"15":166,"16":153},"e_sum":17279,"modal_aux_less_G":1354},"2":{"aux_less_G":599,"d":{"0":69,"1":70,"2":93,"3":120,"4":197,"5":251,"6":300,"7":300,"8":265,"9":220,"10":111,"11":18,"12":5},"d_sum":12027,"defaults_sum":8160,"e":{"0":46,"1":33,"2":31,"3":24,"4":47,"5":38,"6":60,"7":44,"8":42,"9":57,"10":60,"11":61,"12":115,"13":140,"14":190,"15":276,"16":755},"e_sum":25413,"modal_aux_less_G":1185},"4":{"aux_less_G":146,"d":{"0":21,"1":15,"2":37,"3":53,"4":120,"5":155,"6":245,"7":264,"8":266,"9":301,"10":244,"11":182,"12":82,"13":33,"14":1},"d_sum":15527,"defaults_sum":8160,"e":{"0":14,"1":4,"2":3,"3":4,"4":10,"5":9,"6":9,"7":11,"8":6,"9":15,"10":16,"11":31,"12":21,"13":37,"14":71,"15":152,"16":1606},"e_sum":30625,"modal_aux_less_G":999},"8":{"aux_less_G":14,"d":{"1":8,"2":20,"3":31,"4":81,"5":120,"6":219,"7":206,"8":221,"9":228,"10":235,"11":252,"12":180,"13":165,"14":53},"d_sum":17810,"defaults_sum":8160,"e":{"6":1,"7":2,"8":5,"9":3,"10":2,"11":3,"12":3,"13":6,"14":6,"15":18,"16":1970},"e_sum":32128,"modal_aux_less_G":874}}},"calls":{"all_full_calls":60570,"positive_full_calls":12736,"positive_nonself_calls":12706,"positive_self_calls":30,"self_full_calls":30,"singleton_calls":30,"singleton_positive_calls":0,"singleton_self_calls":0,"zero_full_calls":47834},"data_path":"research_algorithms_20260920/experiments/data/facebook_combined.txt.gz","data_sha256":"125e84db872eeba443d270c70315c256b0af43a502fcfe51f50621166ad035d7","name":"Facebook","nonself_positive_r":{"0":4905,"1":2382,"2":1479,"3":1278,"4":976,"5":930,"6":382,"7":206,"8":93,"9":45,"10":30},"nonself_positive_s":{"1":6230,"2":1827,"3":902,"4":515,"5":339,"6":268,"7":258,"8":173,"9":170,"10":209,"11":129,"12":118,"13":141,"14":174,"15":246,"16":1007},"normalization":{"duplicate_rows":0,"edges":88234,"memberships":176468,"nodes":4039,"raw_rows":88234,"self_loop_rows":0},"physical":{"disjoint_pairs":3,"full_pairs":2019,"max_shared":169,"shared_memberships":12651},"positive_calls_per_block":{"0":130,"2":25,"3":89,"4":268,"5":237,"6":34,"7":527,"8":291,"9":374,"10":16,"11":2,"12":5,"13":15,"14":2,"15":1,"20":2,"25":1},"queried_full_pairs":{"K":{"1":728,"3":6,"4":349,"5":4,"6":39,"7":182,"8":15,"9":76,"10":110,"11":25,"12":57,"13":74,"14":60,"15":38,"16":34,"17":32,"18":14,"19":14,"20":10,"21":11,"22":3,"23":2,"24":3,"25":2,"26":1},"R":{"1":728,"2":6,"3":128,"4":254,"5":81,"6":145,"7":132,"8":106,"9":112,"10":87,"11":55,"12":34,"13":16,"14":5},"n":1889,"r":{"0":728,"1":355,"2":224,"3":195,"4":142,"5":133,"6":59,"7":31,"8":12,"9":6,"10":4},"tau":{"1":{"aux_less_G":1257,"d":{"0":196,"1":166,"2":202,"3":217,"4":262,"5":280,"6":263,"7":195,"8":93,"9":11,"10":4},"d_sum":7495,"defaults_sum":7673,"e":{"0":146,"1":98,"2":98,"3":102,"4":79,"5":98,"6":98,"7":115,"8":110,"9":119,"10":106,"11":112,"12":100,"13":129,"14":118,"15":143,"16":118},"e_sum":15604,"modal_aux_less_G":1267},"2":{"aux_less_G":595,"d":{"0":69,"1":67,"2":88,"3":116,"4":185,"5":235,"6":268,"7":284,"8":248,"9":202,"10":106,"11":16,"12":5},"d_sum":11200,"defaults_sum":7673,"e":{"0":46,"1":33,"2":31,"3":24,"4":47,"5":38,"6":60,"7":44,"8":42,"9":57,"10":60,"11":61,"12":114,"13":136,"14":179,"15":263,"16":654},"e_sum":23384,"modal_aux_less_G":1107},"4":{"aux_less_G":146,"d":{"0":21,"1":14,"2":33,"3":52,"4":112,"5":141,"6":219,"7":249,"8":251,"9":283,"10":234,"11":169,"12":79,"13":31,"14":1},"d_sum":14565,"defaults_sum":7673,"e":{"0":14,"1":4,"2":3,"3":4,"4":10,"5":9,"6":9,"7":11,"8":6,"9":15,"10":16,"11":31,"12":21,"13":37,"14":71,"15":152,"16":1476},"e_sum":28545,"modal_aux_less_G":929},"8":{"aux_less_G":14,"d":{"1":7,"2":16,"3":30,"4":73,"5":107,"6":195,"7":189,"8":208,"9":212,"10":226,"11":238,"12":176,"13":159,"14":53},"d_sum":16820,"defaults_sum":7673,"e":{"6":1,"7":2,"8":5,"9":3,"10":2,"11":3,"12":3,"13":6,"14":6,"15":18,"16":1840},"e_sum":30048,"modal_aux_less_G":805}}},"receipt_path":"research_algorithms_20260920/Similarity-Overlap-Facebook-Results.json","receipt_sha256":"2f78c480d64d1c8df4f307bbfe992a13c767a287f634b46c57e9501ee21336ba","reproduced_posting_totals":{"block_memberships":155786,"blocks_seen":12736},"scoring":false,"tau":{"1":{"calls":{"active":5215,"active_aux_less_G":2532,"active_modal_aux_less_G":3647,"admitted":9105,"aux_less_G":5843,"d_sum":36744,"e_sum":77451,"inactive":3890,"modal_aux_less_G":6063,"refused":3601,"source_ids_with_active":30,"source_ids_with_admitted":30},"positive_self_calls":{"active":2,"admitted":2,"refused":28}},"2":{"calls":{"active":6270,"active_aux_less_G":996,"active_modal_aux_less_G":3944,"admitted":10493,"aux_less_G":3260,"d_sum":62169,"e_sum":130319,"inactive":4223,"modal_aux_less_G":6129,"refused":2213,"source_ids_with_active":30,"source_ids_with_admitted":30},"positive_self_calls":{"active":11,"admitted":11,"refused":19}},"4":{"calls":{"active":7078,"active_aux_less_G":112,"active_modal_aux_less_G":3779,"admitted":11390,"aux_less_G":856,"d_sum":87430,"e_sum":172003,"inactive":4312,"modal_aux_less_G":5532,"refused":1316,"source_ids_with_active":30,"source_ids_with_admitted":30},"positive_self_calls":{"active":16,"admitted":16,"refused":14}},"8":{"calls":{"active":7759,"active_aux_less_G":3,"active_modal_aux_less_G":3719,"admitted":12083,"aux_less_G":95,"d_sum":108042,"e_sum":192179,"inactive":4324,"modal_aux_less_G":5005,"refused":623,"source_ids_with_active":30,"source_ids_with_admitted":30},"positive_self_calls":{"active":26,"admitted":26,"refused":4}}},"unique_exact_calls":0}
{"G":64,"all_full_pairs":{"K":{"1":192,"3":5,"4":216,"6":12,"7":170,"8":3,"9":21,"10":148,"11":8,"12":24,"13":114,"14":12,"15":47,"16":89,"17":7,"18":53,"19":88,"20":20,"21":48,"22":59,"23":27,"24":40,"25":49,"26":34,"27":40,"28":46,"29":35,"30":25,"31":30,"32":18,"33":24,"34":20,"35":27,"36":22,"37":15,"38":23,"39":19,"40":22,"41":14,"42":12,"43":8,"44":9,"45":16,"46":8,"47":5,"48":10,"49":6,"50":12,"51":8,"52":11,"53":6,"54":6,"55":5,"56":5,"57":3,"59":5,"60":4,"61":3,"62":4,"63":2,"65":3,"66":1,"71":1},"R":{"1":192,"2":5,"3":38,"4":186,"5":22,"6":57,"7":129,"8":40,"9":75,"10":90,"11":47,"12":72,"13":73,"14":57,"15":80,"16":78,"17":62,"18":66,"19":65,"20":66,"21":49,"22":56,"23":40,"24":50,"25":44,"26":33,"27":29,"28":31,"29":32,"30":17,"31":24,"32":20,"33":21,"34":12,"35":10,"36":19,"37":8,"38":10,"39":6,"40":3,"41":2,"42":1,"43":1,"44":1},"n":2019,"r":{"0":192,"1":221,"2":182,"3":172,"4":142,"5":150,"6":142,"7":129,"8":109,"9":106,"10":91,"11":67,"12":65,"13":53,"14":45,"15":27,"16":29,"17":19,"18":22,"19":17,"20":12,"21":8,"22":6,"23":7,"24":2,"25":2,"27":1,"28":1},"tau":{"1":{"aux_less_G":349,"d":{"0":2,"1":5,"2":5,"3":11,"4":19,"5":20,"6":23,"7":34,"8":39,"9":31,"10":31,"11":49,"12":58,"13":54,"14":82,"15":85,"16":77,"17":90,"18":139,"19":122,"20":127,"21":126,"22":131,"23":109,"24":98,"25":110,"26":78,"27":78,"28":61,"29":50,"30":32,"31":25,"32":15,"33":1,"34":1,"35":1},"d_sum":38729,"defaults_sum":24695,"e":{"6":2,"8":1,"9":3,"10":4,"12":7,"14":1,"15":1,"16":4,"17":1,"18":1,"19":4,"20":2,"21":3,"22":5,"23":3,"24":1,"25":4,"27":4,"28":6,"29":6,"30":7,"31":5,"32":10,"33":9,"34":8,"35":13,"36":12,"37":7,"38":8,"39":15,"40":14,"41":19,"42":12,"43":20,"44":17,"45":29,"46":18,"47":35,"48":21,"49":34,"50":36,"51":39,"52":43,"53":44,"54":46,"55":56,"56":57,"57":87,"58":93,"59":108,"60":131,"61":137,"62":171,"63":243,"64":352},"e_sum":112917,"modal_aux_less_G":1597},"2":{"aux_less_G":17,"d":{"1":4,"2":4,"3":4,"4":11,"5":10,"6":16,"7":22,"8":18,"9":21,"10":17,"11":24,"12":38,"13":38,"14":41,"15":45,"16":52,"17":49,"18":60,"19":64,"20":79,"21":86,"22":89,"23":85,"24":96,"25":89,"26":78,"27":91,"28":80,"29":91,"30":87,"31":89,"32":74,"33":63,"34":70,"35":53,"36":40,"37":52,"38":31,"39":23,"40":18,"41":11,"42":4,"43":1,"44":1},"d_sum":49403,"defaults_sum":24695,"e":{"33":1,"34":1,"35":1,"36":4,"38":5,"39":2,"40":2,"42":1,"43":2,"44":3,"45":2,"46":1,"47":2,"48":5,"49":3,"50":4,"51":4,"52":4,"53":6,"54":7,"55":9,"56":8,"57":12,"58":14,"59":27,"60":28,"61":55,"62":81,"63":190,"64":1535},"e_sum":127078,"modal_aux_less_G":1436},"4":{"aux_less_G":1,"d":{"1":2,"2":4,"3":4,"4":11,"5":10,"6":15,"7":22,"8":17,"9":18,"10":17,"11":21,"12":33,"13":35,"14":35,"15":41,"16":40,"17":36,"18":46,"19":49,"20":65,"21":77,"22":69,"23":59,"24":76,"25":68,"26":64,"27":80,"28":63,"29":70,"30":65,"31":76,"32":78,"33":47,"34":66,"35":40,"36":51,"37":60,"38":37,"39":47,"40":43,"41":43,"42":29,"43":38,"44":33,"45":30,"46":27,"47":17,"48":20,"49":7,"50":11,"51":5,"52":1,"55":1},"d_sum":55645,"defaults_sum":24695,"e":{"61":3,"62":3,"63":15,"64":1998},"e_sum":129186,"modal_aux_less_G":1363},"8":{"aux_less_G":0,"d":{"1":1,"2":4,"3":4,"4":11,"5":10,"6":15,"7":22,"8":17,"9":18,"10":17,"11":21,"12":33,"13":35,"14":35,"15":41,"16":40,"17":36,"18":46,"19":49,"20":64,"21":77,"22":70,"23":59,"24":74,"25":68,"26":62,"27":78,"28":65,"29":70,"30":62,"31":72,"32":77,"33":50,"34":63,"35":40,"36":48,"37":55,"38":37,"39":46,"40":40,"41":34,"42":31,"43":27,"44":34,"45":31,"46":27,"47":25,"48":23,"49":17,"50":17,"51":9,"52":5,"53":4,"55":1,"56":2},"d_sum":56224,"defaults_sum":24695,"e":{"64":2019},"e_sum":129216,"modal_aux_less_G":1353}}},"calls":{"all_full_calls":60570,"positive_full_calls":12736,"positive_nonself_calls":12706,"positive_self_calls":30,"self_full_calls":30,"singleton_calls":30,"singleton_positive_calls":0,"singleton_self_calls":0,"zero_full_calls":47834},"data_path":"research_algorithms_20260920/experiments/data/facebook_combined.txt.gz","data_sha256":"125e84db872eeba443d270c70315c256b0af43a502fcfe51f50621166ad035d7","name":"Facebook","nonself_positive_r":{"0":1097,"1":1309,"2":1064,"3":1075,"4":868,"5":887,"6":841,"7":768,"8":767,"9":669,"10":579,"11":495,"12":458,"13":379,"14":312,"15":192,"16":213,"17":142,"18":166,"19":132,"20":91,"21":58,"22":48,"23":52,"24":14,"25":14,"27":8,"28":8},"nonself_positive_s":{"1":6144,"2":1734,"3":918,"4":490,"5":297,"6":259,"7":207,"8":160,"9":133,"10":127,"11":125,"12":96,"13":98,"14":104,"15":63,"16":61,"17":48,"18":52,"19":62,"20":45,"21":45,"22":51,"23":53,"24":30,"25":40,"26":36,"27":36,"28":36,"29":38,"30":25,"31":44,"32":42,"33":46,"34":41,"35":25,"36":31,"37":23,"38":26,"39":22,"40":25,"41":21,"42":26,"43":24,"44":21,"45":30,"46":27,"47":34,"48":35,"49":26,"50":36,"51":29,"52":29,"53":29,"54":36,"55":37,"56":31,"57":45,"58":36,"59":48,"60":53,"61":46,"62":41,"63":18,"64":10},"normalization":{"duplicate_rows":0,"edges":88234,"memberships":176468,"nodes":4039,"raw_rows":88234,"self_loop_rows":0},"physical":{"disjoint_pairs":3,"full_pairs":2019,"max_shared":169,"shared_memberships":12651},"positive_calls_per_block":{"0":130,"2":25,"3":89,"4":268,"5":237,"6":34,"7":527,"8":291,"9":374,"10":16,"11":2,"12":5,"13":15,"14":2,"15":1,"20":2,"25":1},"queried_full_pairs":{"K":{"1":173,"3":4,"4":191,"6":10,"7":161,"8":2,"9":19,"10":137,"11":7,"12":22,"13":105,"14":11,"15":42,"16":85,"17":5,"18":47,"19":78,"20":20,"21":46,"22":57,"23":23,"24":40,"25":49,"26":33,"27":36,"28":45,"29":34,"30":24,"31":29,"32":17,"33":23,"34":19,"35":27,"36":22,"37":15,"38":23,"39":19,"40":22,"41":14,"42":12,"43":8,"44":9,"45":16,"46":8,"47":5,"48":10,"49":6,"50":12,"51":8,"52":11,"53":6,"54":6,"55":5,"56":5,"57":3,"59":5,"60":4,"61":3,"62":4,"63":2,"65":3,"66":1,"71":1},"R":{"1":173,"2":4,"3":32,"4":165,"5":21,"6":49,"7":122,"8":34,"9":67,"10":87,"11":42,"12":68,"13":68,"14":52,"15":73,"16":74,"17":57,"18":65,"19":61,"20":63,"21":47,"22":55,"23":40,"24":48,"25":43,"26":33,"27":28,"28":31,"29":32,"30":17,"31":24,"32":20,"33":21,"34":12,"35":10,"36":19,"37":8,"38":10,"39":6,"40":3,"41":2,"42":1,"43":1,"44":1},"n":1889,"r":{"0":173,"1":195,"2":171,"3":158,"4":130,"5":140,"6":125,"7":124,"8":106,"9":101,"10":87,"11":64,"12":64,"13":53,"14":45,"15":27,"16":29,"17":19,"18":22,"19":17,"20":12,"21":8,"22":6,"23":7,"24":2,"25":2,"27":1,"28":1},"tau":{"1":{"aux_less_G":346,"d":{"0":2,"1":4,"2":4,"3":10,"4":15,"5":17,"6":17,"7":28,"8":36,"9":28,"10":27,"11":46,"12":50,"13":46,"14":79,"15":81,"16":74,"17":84,"18":132,"19":112,"20":123,"21":118,"22":126,"23":104,"24":93,"25":105,"26":75,"27":77,"28":58,"29":49,"30":30,"31":22,"32":14,"33":1,"34":1,"35":1},"d_sum":36601,"defaults_sum":23687,"e":{"6":2,"8":1,"9":3,"10":4,"12":7,"14":1,"15":1,"16":4,"17":1,"18":1,"19":4,"20":2,"21":3,"22":5,"23":3,"24":1,"25":4,"27":4,"28":6,"29":6,"30":7,"31":5,"32":10,"33":9,"34":8,"35":13,"36":12,"37":7,"38":8,"39":15,"40":14,"41":19,"42":12,"43":20,"44":17,"45":29,"46":18,"47":35,"48":21,"49":34,"50":36,"51":39,"52":43,"53":44,"54":46,"55":56,"56":56,"57":86,"58":91,"59":99,"60":126,"61":125,"62":157,"63":228,"64":281},"e_sum":104768,"modal_aux_less_G":1471},"2":{"aux_less_G":17,"d":{"1":3,"2":3,"3":3,"4":7,"5":8,"6":9,"7":16,"8":15,"9":18,"10":14,"11":21,"12":30,"13":30,"14":38,"15":42,"16":50,"17":44,"18":54,"19":58,"20":76,"21":77,"22":87,"23":80,"24":92,"25":86,"26":75,"27":88,"28":77,"29":88,"30":85,"31":85,"32":73,"33":62,"34":69,"35":51,"36":37,"37":50,"38":30,"39":23,"40":18,"41":11,"42":4,"43":1,"44":1},"d_sum":47065,"defaults_sum":23687,"e":{"33":1,"34":1,"35":1,"36":4,"38":5,"39":2,"40":2,"42":1,"43":2,"44":3,"45":2,"46":1,"47":2,"48":5,"49":3,"50":4,"51":4,"52":4,"53":6,"54":7,"55":9,"56":8,"57":12,"58":14,"59":27,"60":28,"61":55,"62":81,"63":190,"64":1405},"e_sum":118758,"modal_aux_less_G":1311},"4":{"aux_less_G":1,"d":{"1":2,"2":3,"3":3,"4":7,"5":8,"6":8,"7":16,"8":14,"9":15,"10":14,"11":18,"12":25,"13":27,"14":32,"15":38,"16":38,"17":31,"18":40,"19":43,"20":62,"21":68,"22":66,"23":55,"24":72,"25":64,"26":61,"27":77,"28":60,"29":67,"30":63,"31":72,"32":77,"33":46,"34":65,"35":38,"36":48,"37":58,"38":36,"39":47,"40":43,"41":43,"42":29,"43":38,"44":33,"45":30,"46":27,"47":17,"48":20,"49":7,"50":11,"51":5,"52":1,"55":1},"d_sum":53284,"defaults_sum":23687,"e":{"61":3,"62":3,"63":15,"64":1868},"e_sum":120866,"modal_aux_less_G":1238},"8":{"aux_less_G":0,"d":{"1":1,"2":3,"3":3,"4":7,"5":8,"6":8,"7":16,"8":14,"9":15,"10":14,"11":18,"12":25,"13":27,"14":32,"15":38,"16":38,"17":31,"18":40,"19":43,"20":61,"21":68,"22":67,"23":55,"24":70,"25":64,"26":59,"27":75,"28":62,"29":67,"30":60,"31":68,"32":76,"33":49,"34":62,"35":38,"36":45,"37":53,"38":36,"39":46,"40":40,"41":34,"42":31,"43":27,"44":34,"45":31,"46":27,"47":25,"48":23,"49":17,"50":17,"51":9,"52":5,"53":4,"55":1,"56":2},"d_sum":53863,"defaults_sum":23687,"e":{"64":1889},"e_sum":120896,"modal_aux_less_G":1228}}},"receipt_path":"research_algorithms_20260920/Similarity-Overlap-Facebook-Results.json","receipt_sha256":"2f78c480d64d1c8df4f307bbfe992a13c767a287f634b46c57e9501ee21336ba","reproduced_posting_totals":{"block_memberships":155786,"blocks_seen":12736},"scoring":false,"tau":{"1":{"calls":{"active":9179,"active_aux_less_G":1308,"active_modal_aux_less_G":7377,"admitted":10268,"aux_less_G":1794,"d_sum":196771,"e_sum":570327,"inactive":1089,"modal_aux_less_G":7947,"refused":2438,"source_ids_with_active":30,"source_ids_with_admitted":30},"positive_self_calls":{"active":7,"admitted":7,"refused":23}},"2":{"calls":{"active":10349,"active_aux_less_G":65,"active_modal_aux_less_G":7402,"admitted":11443,"aux_less_G":90,"d_sum":285732,"e_sum":719343,"inactive":1094,"modal_aux_less_G":7800,"refused":1263,"source_ids_with_active":30,"source_ids_with_admitted":30},"positive_self_calls":{"active":18,"admitted":18,"refused":12}},"4":{"calls":{"active":11115,"active_aux_less_G":7,"active_modal_aux_less_G":7478,"admitted":12209,"aux_less_G":11,"d_sum":343639,"e_sum":781163,"inactive":1094,"modal_aux_less_G":7790,"refused":497,"source_ids_with_active":30,"source_ids_with_admitted":30},"positive_self_calls":{"active":26,"admitted":26,"refused":4}},"8":{"calls":{"active":11603,"active_aux_less_G":0,"active_modal_aux_less_G":7630,"admitted":12697,"aux_less_G":0,"d_sum":365465,"e_sum":812608,"inactive":1094,"modal_aux_less_G":7935,"refused":9,"source_ids_with_active":30,"source_ids_with_admitted":30},"positive_self_calls":{"active":27,"admitted":27,"refused":3}}},"unique_exact_calls":0}
{"old_helper_sha256": {"research_algorithms_20260920/experiments/bench_bounded_seed_similarity.py": "7bfd5b3cfbba9e97f9efa233f235fd8ab38a1cbf05ed5e4638cb3c0e3748bbe3", "research_algorithms_20260920/experiments/probe_laminar_capacity_similarity.py": "feff9868b220dd8a775447d79648f8ec510ab099a6eb5b67efa2da9768a12537", "research_algorithms_20260920/experiments/probe_orientation_parameter_similarity.py": "5f8370f506306918193e0e019cc22b84488b5d2bd81b767b571ab02c687c4779", "research_algorithms_20260920/experiments/probe_overlap_frontier_similarity.py": "db4adc5334865f7758d808ebe56682188ba0e54f93a83d32eae40fee4c4cacee", "research_algorithms_20260920/experiments/probe_selective_capacity_similarity.py": "961594203c21e9d953824109b4c1356f91527b79b9bc9c96161cbab68b2e9a46"}, "signature_equivalence": {"local_profiles": 594, "query_points": 2970, "unordered_pairs_with_diagonal": 19899}, "total_unique_exact_calls": 12542}
```

## Exact Counts And Scope

The recorded run uses 60 frozen source IDs, 4640 distinct full pairs and one
singleton tail. Across G=4,16,64 it counts 417600 source/full-pair combinations
and 90 singleton combinations, yielding 42339 positive nonself calls across
the three groupings. There are 24 dataset/G/tau profile cells, but only 12542
unique exact score evaluations, all at G=4. Local signature checks cover 594
profiles, 2970 query points and 19899 profile pairs. All assertions passed.

Changed path: `research_algorithms_20260920/Similarity-Exception-Workload-Profile.md`
only. No lead implementation/tests, other documents, data, generated indexes,
or receipts were changed; no full suite, commit, push, timing or RSS claim.

**Verdict:** fixed tau exceptions do not substantiate the general memory pitch;
modal signatures remain mathematically exact and empirically more compact by
the stated entry-count proxy. G=4 shows limited additional information beyond
the existing interval filter. Larger-G modal integration remains conditional
on bounded-r behavior, an actual representation accounting, and incremental
pruning evidence not measured here.

# Similarity And Embeddings: Independent Skeptical Review

Date: 2026-09-20. Independent review of A05 and A06, not author revisions, an implementation endorsement, or a novelty clearance.

## Findings First

**Disposition: retain both as qualified research mechanisms; do not promote either to a demonstrated 4 GB algorithm or an established publishable contribution.** I found no counterexample to the frozen-target similarity theorem or the exact-real embedding recurrence. That does not establish numerical completion, a physical resource bound, or novelty.

Severity here describes the claim being blocked: **P1** blocks a production/performance/publication claim; **P2** identifies an important contract or validation gap. These are not all implementation bugs. Several are limitations the authors already acknowledge. No P0 or demonstrated false published answer was found under the stated contracts.

### IR-01 [P1] A06 Can Fail Completion On Ordinary, Default-Profile Inputs

Location: [Replay-Embeddings.md:179](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Replay-Embeddings.md:179), especially the singleton-zero rule and capped retry policy.

There is a stronger failure fixture than an unusually tiny supplied scale. Four nodes share one weight-one feature, so the graph is a loop-free four-clique, `d_i=-1/3`, and `U_i=1/3`. The declared SHA initialization with stable IDs `0,1,2,3`, `d=1`, `r=1`, `g_i=1`, and the 32-byte little-endian seed integer **8** produces `R=(1,-1,0,0)`.

Exact arithmetic gives `Z_0=Z_1=0` and `E_2=(1/9,-1/9,0,0)`. Independent binary64 coefficient enclosures instead produce

```text
Z_1 = [-5.551115123125783e-17, +5.551115123125783e-17]
E_2[2] = E_2[3]
       = [-1.8503717077085944e-17, +1.8503717077085944e-17].
```

With `L=2` and `alpha=(0,0,1)`, the exact output is `(1,-1,0,0)`, but the stipulated norm guard cannot certify the last two rows. The whole job must fail unless an independently admitted fallback succeeds. Increasing finite binary precision shrinks these intervals but does not establish singleton zero: `1/3` remains inexact and independent intervals lose the cancellation dependency.

**This is a completion counterexample, not a false-certificate counterexample.** The conditional enclosure theorem survives. The missing result is a useful completion domain or a bounded dependency-aware/exact-zero fallback, not merely more tests of rational identities. Reproduction appears below.

### IR-02 [P1] A05's Safe-Domain Novelty Threat Is Direct, Not Peripheral

Location: [Certified-Similarity.md:35](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Certified-Similarity.md:35) and its domain construction at line 68.

The full safe-region paper, beyond the metadata inspected by the authors, defines its region by intersecting the dominance half-spaces between **every retained neighbor and every outsider**, and proves retained-neighbor validity inside that intersection. A05 specializes that construction to a one-dimensional integer parameter with affine comparisons. The same paper also discusses impact regions for target insertions/deletions. Thus neither universal pairwise dominance nor safe-region reuse is a new general principle. [Hasan et al., SSTD 2009, section 2 and dynamic-dataset extension](https://www.aamircheema.com/research/SR_SSTD09.pdf).

Signed intersection maintenance also faces direct prior art in dynamic set-kNN. Its local difference indexes, inverted postings, reverse-neighbor maintenance, and fallback/reverification decisions need a mechanism-level comparison. Its batch discussion leaves optimization opportunities, but that is not evidence that this particular combination is new. [Amagata, Hara and Xiao, ICDE 2019, sections III-IV and IV-G](https://ir.library.osaka-u.ac.jp/repo/ouka/all/92851/ProcIntConfDataEng_2019-April_818.pdf).

A defensible candidate contribution is narrower: a bounded certificate shared across different sets, exact discrete Jaccard domain, net-edit correction, and surviving-witness gate, with a demonstrated lifecycle advantage over matched alternatives. The current work has not demonstrated that advantage or established that combination's novelty.

### IR-03 [P1] A06's Algebra Is Established Structured-Recurrence Territory

Location: [Replay-Embeddings.md:100](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Replay-Embeddings.md:100).

The exact identity can be written without graph-specific terminology:

```text
(D + U V^T)^ell R
  = D^ell R + sum_(j=0..ell-1) D^(ell-1-j) U [V^T (D+UV^T)^j R].
```

This is finite recurrence unrolling, not evidence of a new matrix-polynomial identity. Earlier low-rank matrix-function work proves polynomial exactness via Krylov representations; S4 develops diagonal-plus-low-rank recurrence and generating-function evaluation. Neither automatically supplies this graph's physical schedule, but both materially restrict an algebraic novelty claim. [Beckermann, Kressner and Schweitzer, section 3, Theorem 3.2](https://arxiv.org/pdf/1707.03045); [Gu, Goel and Re, S4, sections 3.2-3.3](https://arxiv.org/pdf/2111.00396).

The possible contribution is an exact, incidence-aware execution schedule preserving node-specific initialization and full-row output normalization while avoiding node-layer/norm storage. It needs a nontrivial resource/performance result against factor-aware baselines. A new name for the recurrence is insufficient; neither is an approximate embedding paper a direct substitute for the declared exact output.

### IR-04 [P1] The Optimized External-Memory And Certified Schedules Have Not Been Tested

Location: [similarity-embeddings-Evidence.md:27](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/similarity-embeddings-Evidence.md:27), [Certified-Similarity.md:164](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Certified-Similarity.md:164), and [Replay-Embeddings.md:214](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Replay-Embeddings.md:214).

The author similarity probe builds complete small dictionaries, derives `C` from all targets, checks signed identities separately, and ranks using direct source intersections. It does not execute the external posting reducer, streamed recovery of anchor intersections, bounded selection, or the fallback implementation. Rejected cases use `continue`.

The author embedding probe does build history from its own reconstructed rows, which is meaningful. However, it uses dense oracle/operator arrays and a layer-first replay helper; it does not execute the proposed membership-first gather/rescan layout, bounded row cursors, directed interval kernel, full normalized output publication, or recovery.

I separately tested the **small exact membership-first loop structure** and its logical counters. That closes a narrow algebra/loop-order question, not the external-memory or numerical implementation gap. There is still no tested physical 4 GB run, enforced scratch budget, 50 GB portfolio retention run, or measured source-to-complete-output crossover. Zero algebra mismatches cannot be promoted to any of those claims.

### IR-05 [P2] Refresh Can Erase A05's Entire Amortization Window

Location: [Certified-Similarity.md:210](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Certified-Similarity.md:210).

The draft correctly requires target changes to invalidate universal dominance. This is consequential in self-join use: changing a source that is also a target is not a source-only update. In the initial design, the safe conservative response can be to reverify every anchor certificate against the new snapshot or use the baseline. No tested dependency catalog identifies a cheaper affected subset.

An independent guard-removal counterexample uses `P=S={0}`, old targets `10:{0,1}` and `20:{0,1,2}`, `h=k=1`, and certificate `U={10}`. Replace target 20 with `{0}`. There are no source edit postings, so `C` is empty and the stale gate accepts, returning 10 while the true winner is 20. This does **not** refute the frozen-target theorem; it proves that version validation is part of correctness, not optional metadata.

Measure target churn, old-reader coexistence, and full reverification before claiming incremental self-join benefit. Arbitrary original-ID order or target eligibility changes need the same discipline. A06 likewise needs the stated full count/coefficient/history refresh; singleton-to-pair changes are not locally ignorable.

### IR-06 [P2] Some Tolerances Are Impossible Even With Exact Internal Arithmetic

Location: [Replay-Embeddings.md:98](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Replay-Embeddings.md:98).

For `L=0`, `d=9`, `alpha_0=1`, and a legal supplied all-ones row, each normalized coordinate is exactly `1/3`. Its nearest binary64 value has error `1/54043195528445952`, approximately `1.850371707708594e-17`. Therefore `tau=1e-20` cannot succeed in the selected output format, regardless of precision escalation or zero handling. Binary32 has a larger floor.

The conditional output contract already permits failure, so this is not a logical contradiction. It is a separate admission/reporting requirement: distinguish output-format impossibility, unresolved zero, loose enclosures, overflow, and resource rejection. Retrying every such case as though more internal precision guarantees success wastes the explicitly scarce lifecycle budget.

## What Survives Mathematical Challenge

### A05: Correct Sufficient Certificate, Not A Maximal Reuse Domain

Under fixed target sets, fixed projection and ID order, and exact checked arithmetic, I agree with the proof at [Certified-Similarity.md:58](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Certified-Similarity.md:58):

1. For positive integer `a`, both Jaccard denominators are positive even for counterfactual, unattainable intersections. Cross-multiplication cancels the `c_u*c_v` terms and leaves `(c_u-c_v)*a + c_u*b_v-c_v*b_u`.
2. Since that expression is integral, the losing-ID tie case needs `>=1`; the winning-ID case needs `>=0`. The stated ceiling/floor rules handle both slope signs. The intersection is exactly an integer interval for the **all-U-versus-all-outsiders property**.
3. Signed posting reduction gives the actual intersection correction, including cancellation. Only `Delta_j != 0` belongs to `C`. For an omitted target and every surviving witness, counterfactual scores equal actual scores.
4. At least `k'` distinct unchanged, nonself witnesses dominate each omitted target. Changed witnesses may fall arbitrarily far, but the survivor gate does not rely on them. Sorting the candidate union by the actual exact score and ID therefore recovers full-universe top-k.

The proof handles zero tails, duplicate feature sets with different IDs, source ID present/absent, and score ties without requiring tied-output expansion. Empty source and zero-k branches are necessary. Binary cosine's source norm cancels, and squaring is order-preserving only on the declared nonnegative domain; the integer-weighted extension respects that restriction. The stated integer product widths are adequate under the declared bounds, but the probes use unbounded Python integers and do not test checked fixed-width implementations.

Two distinctions matter:

- **Boundary ties are real.** With `P={0,1}`, targets `{0}` and `{0,1,2,3,4}`, and the first target as the sole witness, the upper endpoint is 3 when it has the smaller ID, and 2 when it has the larger ID. The independent probe reproduced both.
- **More witnesses need not admit more queries.** Add target `0:{0,1}` to targets `1:{0}` and `2:{0,1,2,3,4}`. At the anchor, `h=1` chooses `{0}` and is valid throughout positive cardinalities; `h=2` chooses `{0,1}` and is valid only through cardinality 3. For `S={0,1,5,6}`, `k=1`, absent self, and `C=empty`, the one-witness gate accepts and the two-witness gate rejects, although target 0 is still the true winner. This is safe conservatism, not a wrong answer. The interval is not the largest possible top-k reuse region, and witness-size tuning has a genuine tradeoff.

### A06: Correct Elimination And Full-Row Norm Logic

The loop-free diagonal correction is necessary: the diagonal of `UV^T` is `q_i/s_i`, while its off-diagonal entries are exactly the required weighted averages. Thus `P=D+UV^T` holds, with isolates handled separately. The two nested inductions, first over local layers and then over completed feature histories, prove the proposed exact execution.

The `d_i^ell R_i` term genuinely retains node-specific initialization even for identical membership rows. Removing it changes the operator. The algorithm does not need a stored dense `U`, `V`, Gram matrix, or node-layer plane: the local gather slab and frozen feature histories suffice. This is a useful mechanism.

Full-row normalization must remain **after raw propagation**, separately at each layer used in the output sum. Neither normalizing inside propagation nor normalizing one weighted polynomial result is equivalent to equation (2). Reconstructing all coordinates makes the mathematical row norm available; it does not make the numerical zero decision inexpensive. The interval successful-output theorem is sound if every primitive, including endpoint squaring and final conversion/error comparison, actually encloses its exact value. No such implementation was audited here.

An additional conditioning warning follows directly from the declared operator. Its exact propagation has `||P||_infinity <= 1`, but the split expression has

```text
sum_j (|D| + UV^T)_ij = 1 + 2*q_i/s_i <= 3
```

on nonisolates. A straightforward independent-interval error analysis can therefore amplify widths even though the underlying averaging operator is nonexpansive. This is not a claim that every graph realizes exponential error; it explains why stochasticity alone cannot certify replay stability.

## Primary Novelty Comparison

The following are primary papers or author/publication-hosted material inspected during this review. Claims of overlap are distinguished from a proof that the complete proposed schedule already exists. No benchmark numbers are transferred from another machine or output contract.

| Prior work and inspected location | Established overlap and remaining distinction |
| --- | --- |
| [Dynamic Set kNN Self-Join, ICDE 2019](https://ir.library.osaka-u.ac.jp/repo/ouka/all/92851/ProcIntConfDataEng_2019-April_818.pdf), definitions, section III, IV-A, IV-G | Exact Jaccard/cosine maintenance using postings, reverse-kNN and local differences is prior art. Its potentially quadratic local indexes differ from `O(A*h)` retained certificate records, but A05 substitutes paid rescans. It does not establish the proposed batched cross-source certificate's novelty. Its tie convention also needs matching before a benchmark. |
| [Efficient Construction of Safe Regions, SSTD 2009](https://www.aamircheema.com/research/SR_SSTD09.pdf), section 2 and dynamic extension | Universal retained-versus-excluded dominance, region intersection, and target-update handling are established. A05's domain is a scalar integer Jaccard specialization with an edit-support exception set, not the paper's moving Euclidean point. That difference deserves a precise theorem/algorithm comparison, not a broad safe-region claim. |
| [FastRP, CIKM 2019](https://arxiv.org/pdf/1908.11512), section 3.3, Algorithm 1 | Associative projection of weighted transition powers is established. A06 must distinguish elimination of node-layer/norm storage from the already-known avoidance of graph-power materialization. The paper's displayed linear combination does not supply A06's exact per-layer row-normalized output. |
| [RandNE, ICDM 2018](https://zw-zhang.github.io/files/2018_ICDM_RandNE.pdf), Algorithm 1 and section IV-D/Algorithm 3 | Iterative random projection and dynamic updates to intermediate projection results predate A06. Its update method retains previous projection results; that is a storage-versus-recomputation comparison, not a free replacement for A06's feature-history plan. |
| [Learning with Hypergraphs, NeurIPS 2006](https://papers.nips.cc/paper_files/paper/2006/file/dff8e9c2ac33381546d96deea9922999-Paper.pdf), section 4, equation (3) | Factorized incidence propagation is old. Hyperedge-size normalization and self-transitions differ from A06. It is wrong both to claim generic incidence factorization as new and to silently benchmark a different hypergraph operator as exact parity. |
| [Efficient Network Representation Learning via Cluster Similarity, 2023](https://link.springer.com/article/10.1007/s41019-023-00222-x), introduction and method sections | Builds cluster representations from a smaller similarity matrix and derives node representations from them. This is a direct threat to broad claims about smaller shared-state embeddings. It does not establish equality to A06's specific loop-free propagation and node-specific initialization. |
| [FREDE, PVLDB 2021](https://vldb.org/pvldb/vol14/p1102-mottin.pdf), sections 2.6 and 3 | Streaming matrix sketches for graph embeddings with covariance-error guarantees predate A06. Such approximation guarantees are not exact coordinate reproduction; use this as a resource/quality alternative, not an exact-output oracle. |
| [Low-Rank Updates of Matrix Functions, 2017 preprint / 2018 journal](https://arxiv.org/pdf/1707.03045), sections 2-3, Theorem 3.2 | Finite polynomial exactness under low-rank updates and Krylov representations are established. Its stored bases differ from regenerated diagonal rows; the latter's external-memory benefit still needs proof/measurement. Polynomial exactness alone does not cover nonlinear row normalization. |
| [LSSL, NeurIPS 2021](https://papers.neurips.cc/paper_files/paper/2021/file/05546b0e38ab9175cd905eebcc6ebb76-Paper.pdf), Theorem 2 and section 6 | Structured Krylov/power-series evaluation is already a time-space tradeoff topic. The theorem expressly separates exact arithmetic counts from bit complexity and numerical stability; empirical model performance is not evidence that its faster theoretical kernel was implemented. This is especially relevant to interpreting A06's evidence. |

For S4's historical context, [HiPPO, NeurIPS 2020](https://proceedings.neurips.cc/paper/2020/hash/102f0bb6efb3a6128a3c750dd16729be-Abstract.html) establishes online history compression through polynomial projections. LSSL then connects structured recurrences with convolution. [S4, sections 3.1-3.3 and Appendix B](https://arxiv.org/pdf/2111.00396) addresses numerical difficulties in preceding structured approaches and develops DPLR resolvent/Cauchy evaluation. Its low-rank factors are rank one/two in the cited HiPPO construction; A06's feature count can be large. Consequently, S4's near-linear kernel bounds cannot simply be imported into an `F`-feature graph or its nonlinear output contract.

**Novelty conclusion:** these sources eliminate several broad novelty narratives, but do not prove the complete A05/A06 combinations anticipated. The remaining publishable case would require a precise distinction plus either a substantive new theorem or a convincing implemented resource crossover. Current evidence establishes neither. This targeted review is not an exhaustive search of all descendants or unpublished work.

## Resource And Lifecycle Audit

### A05

- The query state can be bounded as proposed **if** the sort buffer, merge fan-in, cursor caches, `h` witnesses, `k'` heap, output chunks and runtime overhead are enforced. Large `k'` is not free merely because `k'<=n`; heap and arithmetic widths must pass admission. Python dictionaries and integers do not demonstrate those byte sizes.
- The `O(A*h)` certificate payload is real, but anchor materialization/selection and target/posting/ID structures are additional state. Two complete target scans and `O(n*h)` dominance comparisons per anchor are not a negligible setup term. Without a resident anchor, `n*B_P` at build time and `q*B_P` during accepted queries are significant.
- Net support is not work. My fixture with 1,000 targets `{0,1,private_j}`, `P={0}`, and `S={1}` has **2,000 posting records and zero net-changed targets**. A single popular edit similarly makes posting traffic linear in the target population. Bounded RAM does not imply sublinear accepted-query work.
- `Sort(X)` is a logical fixed-record transfer model, conditional on a legal fan-in and admitted buffers. File metadata, page/block effects, posting-directory probes including absent features, ID lookup, failed discovery, and output staging must appear in measured totals. Empty postings do not imply zero lookup cost.
- A useful amortization test is `extra_build_and_refresh + Q_rejected*discovery_cost < Q_accepted*(baseline_cost-accepted_cost)`, with matched preparation/output costs handled consistently. The current near-core construction proves eligibility, not that this inequality holds before target refresh.

### A06

- I agree with the resident-history equation `p*F*d*L`, including the current accumulator, and the `O(p*d*L)` gather slab. This is state elimination with an explicit feature-history replacement, not dimension-independent memory. A high-degree row needs the second cursor scan already present in the draft.
- The logical membership count is correct: one scatter at `t=0`, two scans for each later history pass, and one output gather total `2L` for `L>=1`. Feature-vector accumulation work is `m_B*d*[sum_(t=0..L-1)t + L + L] = m_B*d*L*(L+3)/2`. The independent miniature gather/rescan probe confirmed these counts.
- These are **logical** counts. Page alignment, decompression restarts, row-directory access, history memory bandwidth, interval arithmetic, hash regeneration and recovery rereads remain real costs. The 57.6 billion feature-vector accumulation units in the illustrative `L=3` case cannot be inferred to run quickly from its 1.2 GB topology payload traffic.
- At `F=1M,d=128,L=3,p=16`, history alone is 6.144 GB. At `n=100M,d=128`, raw binary32 output is 51.2 GB. Both violate the corresponding resident/retention limits without a different plan. The authors correctly disclose these cases; no extrapolated universal feasibility claim is warranted.
- Wider precision changes both scalar width and arithmetic workspace; restart can repeat every pass. One numerical failure near the last row can waste almost a complete output's work. Peak staging, old/new histories, old/new output generations, and the common portfolio's other retained blocks must be admitted together.
- An important restricted baseline exists when every active feature has the same size: on active rows `d_i=d_0` is constant, hence `Z_(t+1)=(d_0 I+V^T U)Z_t`. This can build histories without replaying all prior node layers; `V^TU` need not be materialized because its action can be evaluated via incidence scans. This does not solve variable-diagonal or numerical issues, but prevents an artificially weak baseline on a favorable uniform-group fixture.

Neither plan currently supplies measured end-to-end deadlines or an enforced peak-scratch implementation. A cap written as an inequality is an admission obligation, not a measurement or proof that every requested input completes.

## Executed Evidence And Coverage

All computations in this review ran through `python3.11` standard input. No production source, fixture, benchmark script, package installation, author file or common journal was written. Small dense oracles were intentional and are not resource evidence.

| Check actually executed | Observation | What it does not establish |
| --- | --- | --- |
| Re-executed author similarity listing | 3,456 interval checks; 285,696 signed identities; 63,460 accepted transfers; 57,888 special cases; 121,348 answer checks; 99,836 fallback cases. Weighted: 1,179 accepted, 243 zero cases, 1,422 checked answers, 5,139 fallback cases. All assertions passed. | Execution of the external reducer, candidate scoring from reduced deltas, or fallback kernel; fixed-width overflow behavior; resource or acceptance-rate prediction. |
| Re-executed strengthened author embedding listing | 4,096 incidence matrices; 147,456 independently built feature cells; 65,536 row/norm checks; 11,664 zero rows; 16,384 layers. All assertions passed. | Numerical normalization, SHA profile, interval enclosures, gather/rescan I/O, or final publication. |
| Independent similarity probe, RNG seed `20260920` | 4,000 random universes, all three metrics: 160,000 direct rational domain comparisons; 990 accepted transfers plus 2,055 special cases; 3,045 answers matched; 8,955 rejected. Included negative/permuted IDs, empty universes, `h=0`, self absent/present, private source features and oversized k. | Exhaustive large domains, a complete storage implementation, or evidence that its acceptance fraction generalizes. |
| Independent exact membership-first embedding probe, RNG seed `620092026` | 300 fixtures with `n=1..6`, `F=0..5`, `d=1..4`, `L=0..5`, weights `1..9`, and signed dyadic initialization; 2,423 row/norm checks, 4,588 logical membership visits, 17,806 feature scalar units. Exact rows and both count equations matched. | File-backed bounded cursors, measured RAM/I/O, certified normalized outputs, checkpoints or retries. |
| Independent adversarial probes | Reproduced ID-sensitive interval endpoints, nonmonotone witness-size eligibility, stale-target misanswer when invalidation is deliberately omitted, cancellation with empty net support, the default-seed zero-certification failure, and the output-format error floor. | A counterexample to either correctly scoped exact algebra theorem. |

The author embedding block was briefly shifted by concurrent author updates; one line-number extraction hit a Markdown fence and failed before running. I switched to fence-based extraction and obtained the successful rerun above. Such launch failures are not counted as algorithm failures or passes.

### Remaining Rejection-Capable Tests

1. **A05 execution parity:** drive the actual bounded posting reduction and candidate scorer, not a direct-intersection substitute; force cancellation across run boundaries, both signs, duplicate input records, fan-in limits, huge rows, empty targets, exact positive ties, self changes, and integer-width limits. Run and compare the actual fallback path.
2. **A05 lifecycle:** interleave target content/size/universe/ID-order updates and source-as-target changes; verify old/new snapshot isolation, full certificate invalidation, slow output, exhausted scratch and restart. Measure accepted/rejected work and amortization independently of edit count.
3. **A06 certified output:** test the seed-8 fixture, exact and near-zero rows, nonzero rows whose intervals cross zero, underflow/overflow/signed zero, irrational norms, negative layer weights, `L=0`, zero-weight skips, both output formats and impossible tolerances. Audit every directed-rounding primitive and use a separate high-precision/exact oracle.
4. **A06 actual schedule:** execute membership-first gather/rescan with bounded row cursors, resident history admission and no hidden degree/norm planes. Test high-degree rows, large `F/n`, increasing depth, precision escalation, mid-pass failure and final-row failure. Include singleton activation/deactivation and changed feature counts.
5. **Comparative result:** use the same operator, initialization, zero/tie rules, output precision, full output volume, prepared-state accounting and refresh cadence. Compare similarity against dynamic set-kNN/safe-reuse and exact search alternatives; compare embeddings against full-layer, incidence-native striped replay and structured special-case baselines. Report completion failures and the full machine's peak memory, not just successful jobs or managed allocation totals.

## Reproduction Of The Strongest New Counterexample

This self-contained listing reproduces the seed and exact binary64 interval endpoints. Rational endpoint operations determine correct outward rounding for this tiny fixture; it is an oracle for the example, **not** an audited production interval library. Feed it to Python 3.11 as standard input.

```python
from fractions import Fraction as Q
from hashlib import sha256
from math import inf, nextafter

def enclose_binary_rational_value(x):
    value = float(x)
    return (
        nextafter(value, -inf) if Q(value) > x else value,
        nextafter(value, inf) if Q(value) < x else value,
    )

def add_outward_interval_values(x, y):
    return (
        enclose_binary_rational_value(Q(x[0]) + Q(y[0]))[0],
        enclose_binary_rational_value(Q(x[1]) + Q(y[1]))[1],
    )

def multiply_outward_interval_values(x, y):
    products = [Q(a) * Q(b) for a in x for b in y]
    return (
        enclose_binary_rational_value(min(products))[0],
        enclose_binary_rational_value(max(products))[1],
    )

seed = (8).to_bytes(32, "little")
R = []
for node in range(4):
    digest = sha256(
        b"DFHR-v1" + seed + node.to_bytes(8, "little")
        + (0).to_bytes(4, "little")
    ).digest()
    value = int.from_bytes(digest[:8], "little")
    R.append(0 if value & 1 else (-1 if (value >> 1) & 1 else 1))
assert R == [1, -1, 0, 0]
positive = enclose_binary_rational_value(Q(1, 3))
negative = (-positive[1], -positive[0])
history = []

def reconstruct_interval_node_layer(node, depth):
    local = (float(R[node]), float(R[node]))
    for layer in range(depth):
        local = add_outward_interval_values(
            multiply_outward_interval_values(negative, local),
            multiply_outward_interval_values(positive, history[layer]),
        )
    return local

for depth in range(2):
    aggregate = (0.0, 0.0)
    for node in range(4):
        aggregate = add_outward_interval_values(
            aggregate, reconstruct_interval_node_layer(node, depth)
        )
    history.append(aggregate)

rows = [reconstruct_interval_node_layer(node, 2) for node in range(4)]
assert history[0] == (0.0, 0.0)
assert history[1][0] < 0 < history[1][1]
assert rows[2][0] < 0 < rows[2][1]
print("R", R, "Z1", history[1])
print("E2 exact", [str(Q(value, 9)) for value in R])
print("E2 interval", rows)
error = abs(Q(float(Q(1, 3))) - Q(1, 3))
assert error > Q(1, 10**20)
print("nearest binary64 error for 1/3", error)
```

## Review Scope And Final Assessment

Read all three requested documents. Author work continued concurrently; I reread the updated embedding contract and strengthened evidence before finalizing. Last reviewed SHA-256 fingerprints:

```text
Certified-Similarity.md
8e0db90382bb16b0970378249c800eb90a1b0814ebbece9b028b443020e993aa
Replay-Embeddings.md
4a84afaee2f750637490ba6b1647b6aba2e9af04bbee7e2fe5e3a08c206459a2
similarity-embeddings-Evidence.md
0dbe10f02c7dc1cdd52ecefe39f41426728a570719a5d57a2b9a8a497e77d7e3
```

Only this review file was authored by this review pass. The surrounding research directory was already untracked; no author changes were reverted or staged. Primary-web inspection was targeted, not full-paper replication. The EPFL-hosted matrix-function PDF timed out; its arXiv copy was inspected instead. No claims rely on a blocked page.

**A05:** mathematically sound, useful sufficient reuse mechanism for an explicitly frozen target universe. Publishable contribution remains unestablished because the safe-region and dynamic-maintenance overlap is strong and amortization is unmeasured.

**A06:** mathematically sound state-elimination/reconstruction mechanism for the declared incidence operator. Numerical completion fails on a tiny legal SHA-profile fixture under straightforward interval execution; the optimized external schedule and physical crossover remain untested. Publication would need more than the identity, while deployment additionally needs a dependable completion policy.

**Bottom line:** keep the correct mechanisms, preserve the explicit caveats, and treat the next milestone as adversarial execution and comparative evidence. Do not relabel exact small-oracle success as resource proof or established algorithmic novelty.

## Follow-Up: New A06 Completion Guarantees

Date: 2026-09-20. Scope is only the new diagonal-group closure, group-zero route, scaled-integer propagation, finite output theorem, and fourth evidence fence. No A05 review or novelty reassessment is included. This addendum leaves the original review intact as a historical snapshot.

**Verdict:** no counterexample found to the new recurrence, exact group-zero certificate, propagation-width bound, or conditional finite-output theorem. The new exact routes resolve the earlier seed-8 completion failure within their admitted domains. Two P2 execution/admission gaps remain in the evidence; neither is a demonstrated wrong published answer. The bounds can also be strengthened materially.

### Follow-Up Findings

**A06-F1 [P2]: The fourth fence's no-Q test does not exercise independent zero-route admission.**

The theorem at [Replay-Embeddings.md:250](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Replay-Embeddings.md:250) is valid: exact zero in every initial group implies zero at every later group, so active rows only need the parity-weighted normalized initial vector. Isolates use `alpha_0` alone. Neither transition-denominator construction nor propagated integer powers are mathematically necessary.

However, [the fourth fence's preparer](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/similarity-embeddings-Evidence.md:716) constructs and admits `Q`, the general history scalar count, and propagation width before either zero helper runs. [Deleting `data.Q` afterward](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/similarity-embeddings-Evidence.md:1015) proves the helper does not read it, not that preparation can bypass an inadmissible Q/history.

Exact admission counterexample: disjoint unit-weight stars with degrees `2,3,5,7,11,13`, center initialization `+1`, and every leaf `-1`. There are 47 nodes, 41 features, one diagonal group, and every feature's initial group moment is zero. `W_initial=2+bit_length(47)=8`, but the raw common denominator is 30,030. Invoking the actual fourth-fence `Incidence(..., L=8, bits_cap=8)` fails with **`denominator admission`**, before the zero certificate can run. The independently admitted zero route needs no such denominator. This is a counterexample to treating the current test preparer as the advertised admission dispatcher, not to the certificate theorem. Test zero discovery before general-route Q/history admission, with separately sufficient output workspace.

**A06-F2 [P2]: Endpoint and propagation bounds must not be reused as bounds on all arithmetic temporaries.**

The qualification about separate scratch at [Replay-Embeddings.md:294](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Replay-Embeddings.md:294) is essential. Two exact examples make its required size concrete:

- A pair joined by one feature of weight `2^32-1`, with `M=1,L=1`, has `Q=2^32-1` and propagation width `W=37`. The literal coefficient expression `Q*w_f/s_i` first constructs a **64-bit magnitude** although its final result fits W. Evaluate `(Q//s_i)*w_f` under the current raw-Q definition, or explicitly admit the wider product. A post-addition width assertion does not measure this temporary.
- For a unit-weight two-node pair with initial rows `(1,1)` and `(1,2)`, `L=1`, `alpha=(1/2,1/2)`, binary64 output, and `tau=2^-51`, the sufficient output condition holds. The selected precision is `b=54`, propagation `W=6`, and endpoint denominator budget `B_y=129`. The first output coordinate has endpoint denominator bit lengths **110 and 107**, but its exact midpoint denominator needs **216 bits**. The shifted-norm operand budget is only 121 bits in this fixture. Neither B_y nor the norm scratch alone bounds midpoint construction.

This does not refute B_y as an **endpoint** bound or the theorem's explicit requirement that all workspace fit. It identifies what the admission calculation must include. With endpoint denominator budget B, generic midpoint formation needs a denominator budget up to `2B+1` bits, plus the corresponding numerator/cross-product and gcd scratch. Exact binary64 endpoint-distance checks can introduce a float denominator `2^1074`; binary32 uses `2^149`. Charge these format-dependent intermediates too, or use an explicitly bounded alternative conversion/check algorithm.

The B_y argument assumes reduced rational accumulators or a representation that keeps the common alpha denominator factored once. Repeated unreduced fraction addition can duplicate `2^E_alpha` in temporaries. Specify that representation and its scratch; the fourth fence uses canonical `Fraction` arithmetic but does not enforce B_y, midpoint, or rational-workspace admission caps. Its per-coordinate checks are useful execution evidence, not tests of those caps.

### Recurrence And Partial-Sum Audit

The revised construction at [Replay-Embeddings.md:222](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Replay-Embeddings.md:222) is correct. Multiplying the raw recurrence by `V^T I_g` closes the diagonal term because `I_g D=d_g I_g`. Summing groups recovers the complete aggregate; omitted isolates have zero incidence. Clearing initialization and coefficient denominators then gives equation (6) without changing the operator. Positive `S*Q^t` cancels exactly in normalization.

The one-mutable-group-plane schedule is also valid: freeze the complete `J_t` before scaling any group, and read only that frozen total during all subsequent gathers. Group moments from an interrupted update cannot be reconstructed from total history alone. The `G=1` storage saving requires the separately described uniform constructor to write the next history slot, not to mutate an earlier frozen history array through an alias.

For the width proof, let `t_(g,f)` count members of feature f in group g. Positivity and substochasticity give `|X_tij|<=M Q^t`. Stronger local statements justify the global bound for **every prefix**, not merely final values:

```text
|K_(g,t)[f,j]| <= t_(g,f) M Q^t
|J_t[f,j]|    <= t_f M Q^t
sum_f b_if*t_f = Q*(1+q_i/s_i) <= 2Q
|a_i| = Q*q_i/s_i <= Q.
```

Consequently, every gather product/prefix is bounded by `2 M Q^(t+1)`. A row's diagonal-plus-gather prefix is bounded by `3 M Q^(t+1)`. For a given group/feature accumulator, its scaled old value plus any prefix of row scatters is bounded by `3 t_(g,f) M Q^(t+1)`. A prefix of the reduction of completed groups is bounded by `t_f M Q^t`, because their member sets are disjoint. All fit equation (7) through the requested depth. This argument does not rely on cancellation reducing an oversized intermediate.

If metadata are already admitted, a tighter sufficient propagation factor than `3n` is `C=max(3, max_f t_f, 3*max_(g,f) t_(g,f))`, with maxima over empty sets taken as zero. Then `2+bit_length(max(1,C*M*Q^L))` suffices for these cells and propagation temporaries. This does not bound coefficient preparation, norm, alpha, or output-rational scratch; those remain separate.

### A Smaller Exact Common Denominator

The Q explosion described in the revision is real, but `lcm(s_i)` also includes avoidable weight factors. For every active row define

```text
h_i = gcd { w_f : B_if=1 }
r_i = s_i/h_i
Q_* = lcm { r_i : s_i>0 }.
```

Here `h_i` divides both `q_i` and `s_i`. Q_* is the **smallest common integer clearing all entries of the declared U**, and therefore clears D as well. Sufficiency follows from

```text
b_if = (Q_*//r_i)*(w_f//h_i)
a_i  = -(Q_*//r_i)*(q_i//h_i).
```

For necessity, if an integer Q clears every `w_f/s_i` in a row, an integer linear combination realizing the weights' gcd shows that `Q*h_i/s_i` is integral; thus `r_i` divides Q. These divide-before-multiply formulas also avoid the unreduced `Q*w_f` temporary. Exact grouping and both recurrences are unchanged. Apply the same capped-LCM and width tests with Q_*; this is a bound improvement, not an author-file edit or novelty claim.

Independent example: 16 disjoint pairs, one feature per pair, weighted by the first 16 primes. All rows have diagonal -1 and each U coefficient is exactly 1. With `M=1,L=8`, raw `Q=32589158477190044730` has 65 bits and equation (7) reserves **528 bits per scalar**; Q_* is 1 and the same conservative equation reserves **9 bits**. All-ones initialization makes the grouped moments nonzero, so this is not merely a case already handled by the zero certificate.

This reduction does not solve genuine global-denominator growth: unit-weight stars have `h_i=1`, so their prime center degrees still multiply into Q_*. Width grows roughly as `L*log2(Q_*) + log2(n*M)`, independently of how small G is. Exact parameter admission must use that quantity, not group count as a proxy.

### Output Proof And Tighter Enclosures

I agree with the finite-completion theorem at [Replay-Embeddings.md:298](/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/research_algorithms_20260920/Replay-Embeddings.md:298), under its stated finite-format, correctly rounded conversion, and complete workspace-admission conditions. Its norm enclosure can actually be sharpened by one bit.

For nonzero integer x, let `l=m/2^b` and let u be its upper norm endpoint. Since each `|x_j|` is integral, `l>=max(1,|x_j|)`. Thus every coordinate enclosure already lies in `[-1,1]`, and

```text
width(x_j/[l,u]) = |x_j|*(u-l)/(l*u) <= 2^-b.
```

The draft's `2^(1-b)` is safe but loose. It is sufficient to select `b>=1` with `A*2^-b<=tau/4`. Signed alpha weighting gives final width at most `tau/4`; every intermediate endpoint sum is bounded in magnitude by the corresponding partial sum of `|alpha_t|`, hence by A. This also supports the stated endpoint numerator bound. The zero-certificate route's parity coefficient has magnitude at most A, so the same output argument applies there.

With the draft's original or this tighter precision choice, the true value lies within `tau/8` of the exact midpoint. Because the midpoint is in the finite range `[-A,A]`, correctly rounding it adds at most `rho(A)`. Under `tau>=2*rho(A)`, total error is at most `5*tau/8<tau`. Thus the publication endpoint-error check also succeeds; this is a finite construction, not convergence assumed from repeated retries. Exact zeros and `A=0` need no norm division. The fourth fence's converter passed independent tests around signed underflow, the subnormal/normal boundary, ties, double-rounding-sensitive midpoints, powers of two and the largest finite values.

### Executed Follow-Up Evidence

Only the fourth source fence was replayed for this follow-up; no old similarity or novelty work was rerun. Its execution used temporary files removed by its own context manager. Independent additions used standard-input computations and, for the admission counterexample, the actual fourth-fence definitions extracted in memory.

| Execution | Observed result | Precise scope |
| --- | --- | --- |
| Actual fourth fence | Exit zero; 180 randomized fixtures, 2,397 raw rows, 758 zero rows, 187 successful output publications, and 135 uniform-baseline history matches. | Confirms the reported file-backed grouped constructor, reconstruction, normalized binary output and included adversarial cases. Publication count is not a count of simultaneously retained distinct files. |
| Independent partial-width and reduced-Q probe, seed `2026092006` | 240 fixtures; 375 integer schedule runs using distinct raw/reduced Q choices; 121,363 individual product/prefix checks; 5,279 reconstructed raw rows matched a separately constructed rational operator. Q_* was strictly smaller in 135 fixtures. | Tests both recurrences and partial values, not just final sums. Small exact in-memory oracle; the mathematical proof above supplies the general bound. |
| Independent integer-norm enclosure probe | 14,976 endpoint checks over two-coordinate integers in `[-12,12]`, excluding the zero vector, with `b=1..12`; all satisfy `[-1,1]` containment and the tighter width `2^-b`. | Finite check of the strengthened inequality, not its proof. |
| Actual nearest-format helper against independent ordered-bit binary-search oracle | 200 signed binary32/binary64 rounding cases matched. | Checks conversion boundaries and ties independently of the helper's neighboring-float candidate search. |
| Actual zero-route preparation challenge | 47-node all-zero-moment star family rejected with `denominator admission` despite fitting the 8-bit initial-cell bound. | Demonstrates A06-F1's admission-order gap. |
| Exact scratch probes | Observed the 37-bit-cell/64-bit-coefficient-product example and the 129-bit-endpoint/216-bit-midpoint example. | Demonstrates that distinct temporary budgets cannot be inferred from cell or endpoint bounds. |

The fourth fence does not test pre-admission of the complete sufficient-output domain: it does not enforce `tau>=2*rho(A)` or the stated rational workspace caps, and its dedicated zero helper is entered only after general preparation. Its source explicitly says the integer original-replay fallback is not separately exercised. These limits narrow what the new receipt establishes; they do not negate the correctly scoped recurrence and completion proofs.

### Compact Independent Bound Probe

This standalone Python 3.11 listing retains the denominator and rounding-workspace examples without writing files:

```python
from fractions import Fraction as R
from math import isqrt, lcm

primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53]
Q = lcm(*primes)
n, L = 2*len(primes), 8
print("pair widths", 2+(3*n*Q**L).bit_length(), 2+(3*n).bit_length())
assert Q == 32589158477190044730

weight = 2**32-1
W = 2+(6*weight).bit_length()
print("coefficient temporary", W, (weight*weight).bit_length())
assert W == 37 and (weight*weight).bit_length() == 64

tau, A, b = R(1, 2**51), R(1), 1
assert tau >= 2*(A/2**53 + R(1, 2**1075))
while A*R(2, 2**b) > tau/4:
    b += 1
norm_intervals = []
for norm2 in (2, 5):
    m = isqrt(norm2 << (2*b))
    norm_intervals.append((R(2**b, m+1), R(2**b, m)))
lo = sum(x[0] for x in norm_intervals)/2
hi = sum(x[1] for x in norm_intervals)/2
midpoint = (lo+hi)/2
W, L, E_alpha = 6, 1, 1
B_y = (L+1)*(W+b+1+3)+E_alpha
norm_workspace = 2*W+1+2*b
print("output bits", b, B_y, norm_workspace,
      lo.denominator.bit_length(), hi.denominator.bit_length(),
      midpoint.denominator.bit_length())
assert b == 54 and B_y == 129 and norm_workspace == 121
assert midpoint.denominator.bit_length() == 216
assert max(lo.denominator.bit_length(), hi.denominator.bit_length()) <= B_y

degrees = primes[:6]
n = sum(degrees)+len(degrees)
assert n == 47 and 2+n.bit_length() == 8
assert lcm(*degrees) == 30030 > 2**8-1
print("zero-route admission", n, "nodes; initial width 8; raw Q", lcm(*degrees))
```

Reviewed A06 source SHA-256: `c9aa9546f8844ebe19af9a2bbb036eb7f47b76b0259effd32f8f4add030f0f26`. Reviewed fourth Python fence SHA-256, excluding fence markers and including its newline-terminated source: `d15f48595930d55ed4d4837ce5e087fe53357ea0a73a29ab29d7f6dda351e1ff`. Only this append was authored in the follow-up.

**Updated assessment of these new guarantees:** the exact completion domains are substantive and mathematically supported. Do not carry the prior interval-only seed-8 failure forward as a blocker to them. Implement/test independent zero-route admission and explicitly size conversion/coefficient scratch; consider Q_* and the tighter norm precision before rejecting cases solely on conservative bounds. No new P1 theorem failure was identified in this scoped follow-up.

## Follow-Up: New A05 Outsider Envelope

Date: 2026-09-20. Independent append reviewing only `Similarity-Envelope-Certificate.md`, lines 13-110 and its new Python fence, with a targeted primary-literature comparison. No old A05/A06 test suite was rerun. The subsequent A06 fixes and the separate paid exception-merge extension are outside this review. References below use the reviewed envelope file's line numbers, not the earlier drafts.

### Findings And Disposition

**AE-01 [P2, resource statement] The proposed heap does not establish O(h) threshold work; an equivalent counting gate does.** Lines 69 and 106 disagree for variable k': an h scan with a k'-bounded heap costs O(h log(k'+1)) comparisons in the worst case, while the fence actually sorts the survivors at lines 241-242. Matching ID-sorted U against the already reduced C also reads up to h+q records, where q=|C|. Neither operation is justified by calling the result an order statistic. This is a cost/schedule mismatch, not a counterexample to accepted-answer correctness.

There is a simpler exact repair, requiring no new certificate or exception algorithm: look up E(a) first, stream the unchanged nonself witnesses, and count those strictly ahead of E(a), saturating at k'. Accept exactly when the count reaches k'. For nonempty O and k'>0 this predicate is equivalent to the stated kth-survivor predicate. It takes O(h) score comparisons, O(h+q) sequential survival-matching work, O(log(e+1)) hull lookup, and constant additional threshold state. Final exact candidate scoring/top-k selection still needs its separately charged work and heap; this improvement only removes the threshold heap/sort. The independent contract probe below checked equivalence on 122,880 cases.

**AE-02 [P2, evidence boundary] The new external builder really ran; the complete optimized new query schedule did not.** I independently replayed this new fence and reproduced 128 fixtures, 5,120 envelope checks, 6,400 queries, 1,053 acceptances, 58 strict improvements, sort occupancy four, merge fan-in two, and 211 stack pops. The file-bounded representative sort, hull append/pop, and predecessor lookup are executed mechanisms, not just algebra simulated by a target dictionary. Conversely, lines 238-249 obtain C by exhaustive dictionary intersections, sort W, and score candidates from that dictionary. The fence neither connects the new gate to the bounded signed reducer nor executes its proposed threshold heap. The text acknowledges the oracle-assisted exception production; separate old reducer evidence is not an integration test of this new composition.

The author fence also keeps k below eligible count, U at five, P nonempty, and tests empty O only at the lookup-helper level. My new contract enumeration covers arbitrary U including empty/full, empty anchors, self present/absent, k=0, k beyond eligible count, zero ties, changed witnesses, and counterfactual c>a. It validates the mathematical contract with an oracle, not the missing bounded integration. Before claiming the complete new optimized schedule has passed, run those branches through the actual composed reducer/gate/candidate path and measure its traffic. This is not a request to repeat old tests.

**No P1 accepted-answer or discrete-hull counterexample was found.** The scientific blocker is different: the new certificate has an exact reduction to established linear ranked-query machinery, detailed below. This blocks treating separation from the old all-U gate as a separation from strong prior art. It does not refute the author's explicitly limited claim of a useful combination.

### Exactness And Discrete Boundaries

- **Representatives are correct.** At fixed positive c, minimizing b maximizes the score for every a>=1; equal b is resolved by ID. At c=0 only ID matters. Dropping zero-c rows when a positive-c outsider exists is valid even if that positive row changes or is self: a conservative counterfactual maximum is all the proof needs. Do not remove the winner and treat the next hull record as a certified replacement; that is not this theorem.
- **The integer crossing is correct.** For c_x>c_y, the comparison numerator is `(c_x-c_y)*a+c_x*b_y-c_y*b_x`. It must be at least zero when x wins the equal-score ID tie, otherwise at least one. The ceiling formula follows exactly. Once x beats y it continues to do so as a increases. Thus a stack entry beaten by x at its own start can be popped; an entry first beaten after A_max need not be displaced. Clipping to one is sound. Exhaustive tests of 7,050 three-overlap fixtures with every ID permutation produced 126,900 exact lookup agreements.
- **A tie-only hull entry must survive.** Use `(c,b,ID)=(2,3,2),(3,6,0),(4,9,1)`. The first wins at a=1,2; all three scores equal 1/2 at a=3, so the middle row wins solely by ID; the last wins from a=4. The actual file hull has starts 1,3,4. It remains correct when A_max truncates at 1,2,3,4, or 5. A geometric implementation that drops the middle collinear point without preserving its tie-winning ID is wrong, although the numeric support value is unchanged. The current integer implementation avoids that error.
- **Do not round crossings.** For rows `(1,1,1)` and `(2,9007199254740995,0)`, the first winning cardinality of the latter is exactly 9007199254740993. Binary64 rounds that boundary down by one. The current integer builder returns the correct winner on both sides. This is a regression fixture for future ports, not a failure of the current fence.
- **The accepted-answer theorem is sound under its stated hypotheses.** Every omitted eligible row is in O minus C and has its actual score/ID bounded by E(a). The accepted gate supplies k' distinct eligible witnesses ahead of that bound. Equal numeric scores do not create ambiguity because strictness is in the total score/ID order. Changes to retained witnesses are repaired in C; a changed or self envelope maximizer may cause rejection but cannot cause a false acceptance. Empty O permits bypass because U already covers all targets. k'=0 must return before indexing a kth row. Empty sources and cardinalities outside the admitted interval take the declared baseline, not this proof.
- **A useful additional property is monotonicity in U.** For fixed P, snapshot, S, self, and k', expanding U expands its survivor set and removes outsiders. The kth survivor cannot worsen and the best outsider cannot improve, so acceptance cannot be lost. This compares valid certificates for each U, not a free in-place certificate refresh. The enumeration checked 109,536 accepted nested-U pairs. This precisely strengthens the old all-U behavior without implying a maximal actual-query gate.

The packed arithmetic domain also survives a width check. Put H=2^63-1. A score denominator can reach 2H-1, so form it in a widened type, not signed 64-bit. Each cross product is bounded by H(2H-1)<2^127, and subtracting the two nonnegative products fits signed 128-bit. The crossing numerator has magnitude at most H^2+1; its positive divisor is at most H. The displayed expanded comparison's partial sums fit as well. Use integer ceiling division with defined signed semantics, and compare a crossing to A_max before packing it. This supplies an arithmetic bound; Python's unlimited integers did not test a checked-128 implementation. Byte offsets such as 32e and sort-file lengths require the separate byte/seek admission already called for at line 28.

### Tight Size And Paid Input Work

The O(p) outsider certificate size is correct, not an O(p) build-time claim. It can be sharpened to `e <= min(n_o, max(1,p), A_max)` for the admitted positive integer domain: only positive-c rows survive when any exists; otherwise one zero-c ID suffices; each retained entry occupies at least one integer cardinality. Representative count before dropping zero remains at most p+1.

The dependence on p is unavoidable for this explicit envelope representation. For c=1,...,p, take `(c,b,ID)=(c,c^2,c)`. These are realizable binary targets: retain c anchor features and add c^2-c nonanchor features. The inverse score is `a/c+c-1`. Row c starts winning at `a=c(c-1)+1`; the next row starts at `c(c+1)+1`. With A_max>=p(p-1)+1 all p rows survive. The file builder reproduced this for p=1,2,3,8,32,128. The bound is therefore Theta(p) in a sufficiently large admitted range, not generically constant size. Its 32-byte packed hull alone needs 32p bytes; U, the anchor, indexes and pinned versions remain additional retained state.

An arbitrary supplied outsider stream still needs Omega(n_o) inspections to determine its exact representatives; an unread row can change the winner. The stated resident array can group in O(n_o+p) work after paid overlap discovery. The executed external alternative sorts all n_o rows before reducing them and uses O(n_o log n_o) comparison work. Let D denote anchor-intersection discovery cost: both bounds need D added, and neither makes D small. Candidate rescores, raw signed-posting traffic, the q-row reduced exception stream, output, and fallback are different costs from hull lookup.

For a concrete check, I instrumented only run-file reads/writes in the author's 1,024-outsider, one-hull-record fixture. Its four-record runs and eight two-way merge levels produced **9,216 nonempty 24-byte reads and 9,216 writes**, plus 511 EOF reads: 442,368 transferred triple bytes in total, excluding hull/metadata operations. This matches `(2*8+2)*1024` nonempty record operations. These are Python file-call counts, not physical device I/O or RSS. In particular, `Sort(n_o)` at line 108 must not be read as an experimentally established optimal M/B-way block-sort bound: the fence uses fixed two-way fan-in and unbuffered record calls. The small output certificate and the bounded sort/stack schedule are both real, but neither eliminates preparation traffic.

### Strong Prior-Art Equivalence

The following are primary sources inspected for this extension, not a rerun of the earlier safe-region/dynamic-set-kNN survey.

1. **Linear ranked-query indexing is an exact reduction, not a loose analogy.** Map a positive-c outsider to `v_j=(1/c_j,(b_j-c_j)/c_j)`. Finding E(a) is minimizing `(a,1) dot v_j`, with stable ID as the secondary order. Normalizing weights to `(a/(a+1),1/(a+1))` does not change that answer. Same-c compression removes parallel-line dominated rows; the hull keeps the exposed lower envelope for a restricted one-parameter weight family. Chang et al.'s [Onion technique, SIGMOD 2000, Theorem 1 and Sections 3.1-3.2](https://sigmodrecord.org/publications/sigmodRecord/0006/pdfs/The%20Onion%20Technique_%20Indexing%20for%20Linear%20Optimization%20Queries.pdf) indexes linear optimization with convex-hull layers and stores them on disk. Here only the outsider top-1 support query is needed, not every Onion layer. This reduction and the restricted-domain interpretation are my deductions; that paper does not provide this draft's signed intersection exceptions or integer tie implementation.
2. **A strong skyline comparator must include convex support, not just pairwise pruning.** Xin, Chen and Han's [Towards Robust Indexing for Ranked Queries, VLDB 2006, Sections 2-3](https://hanj.cs.illinois.edu/pdf/vldb06_indexrank.pdf) explicitly develops domination beyond ordinary one-to-one skyline dominance for monotone linear ranking. In this draft, `(1,1,0),(2,7,1),(3,12,2)` give three mutually nondominated transformed points, but the middle point never wins; the hull retains two. The file builder verifies it. Comparing only with a skyline scan would under-equip the comparator. Conversely, a strict-vertex-only convex hull needs ID-aware handling of the tie-only example above. That is a correctness requirement for both implementations, not novelty clearance.
3. **Ranked-view reuse already separates inspected candidates from a bounded unseen tail.** Hristidis and Papakonstantinou's [Algorithms and Applications for Answering Ranked Queries Using Ranked Views, Sections 4-5](https://dbucsd.github.io/paperpdfs/2004_12.pdf) proves watermark-based prefix sufficiency and uses materialized ranked views in PREFER. This is direct prior territory for changing query weights, reusing a prepared ranking, and certifying output against unseen tuples. The new distinction to assess is the overlap-specific finite parameterization and exact exception overlay, not the generic inference that enough candidates ahead of an unseen-tail bound suffice.
4. **The cached-neighbor comparison already uses a result boundary, not every prefetched row.** Li et al.'s [Processing Moving kNN Queries Using Influential Neighbor Sets, Sections 6.1-6.2 and Algorithm 2](https://user.it.uu.se/~wangyi/pdf-files/2014/vldb14.pdf) compares the farthest current result against the nearest guarding object. Its recovery also tries the best k within prefetched R before recomputing. Thus replacing the weakest of all h retained objects with a kth-result boundary is not by itself a new cache-validity principle. Their Euclidean/Voronoi guarding-set construction does not automatically supply this Jaccard envelope, and their geometric guarantees should not be asserted for arbitrary set edits.

**Concrete comparison to require:** an ID-aware two-dimensional support-envelope cache over the same outsider triples, the same U, and the same exact exception reducer. For C empty, its tail query is algebraically identical to E(a); adding the same exception handling and kth-survivor/count gate makes its accept/reject decision identical on every admitted query. It therefore reproduces all 58 strict improvements over the old all-U gate, not merely similar aggregate behavior. This is an equivalence construction of a strong comparator, not an assertion that a cited paper publishes the entire combination verbatim.

What remains useful is the exact reduction, bounded overlap classes, discrete-ID implementation, arbitrary retained-pool sufficiency, and a genuinely executed external builder. A publishable claim still needs either a nontrivial guarantee beyond this composed comparator or a measured systems advantage with preparation, anchor reuse, exception traffic, precision and refresh costs charged equally. The current separating family establishes improvement over the author's older certificate only. The draft already says as much; preserve that restraint.

### Compact New Probe

This retained probe consumes the new author's Python fence on stdin, checks its hash, and loads only its definitions. It never runs old evidence or writes repository artifacts. Its hull checks use temporary binary files; its gate enumeration is deliberately an oracle-level contract check. The separately instrumented I/O counts above were an ephemeral wrapper around this same builder, not a new production implementation. One initial reviewer-written skyline illustration had an incorrect expected size and was replaced by the verified triple above; it was not an author-algorithm failure.

```python
import ast
import sys
from fractions import Fraction as F
from hashlib import sha256
from itertools import combinations, permutations, product
from pathlib import Path
from tempfile import TemporaryDirectory

source = sys.stdin.read()
assert sha256(source.encode()).hexdigest() == "35e46277e36c47ab32b139859146a8238e73c1c33556e07b42b99137697ae367"
tree = ast.parse(source)
cut = next(i for i, node in enumerate(tree.body)
           if isinstance(node, ast.Assign) and any(
               isinstance(t, ast.Name) and t.id == "rng" for t in node.targets))
ns = {}
exec(compile(ast.Module(body=tree.body[:cut], type_ignores=[]), "envelope", "exec"), ns)
build = ns["build_bounded_outsider_envelope"]
lookup = ns["lookup_exact_outsider_envelope"]
key = ns["order_target_counterfactual_key"]
fixtures = checks = 0
with TemporaryDirectory() as tmp:
    root = Path(tmp)
    for cs in combinations(range(5), 3):
        for bs in product(*(range(c, 7) for c in cs)):
            for ids in permutations(range(3)):
                rows = list(zip(cs, bs, ids))
                path, count = build(iter(rows), root, 18)
                for a in range(1, 19):
                    assert lookup(path, count, a) == min(rows, key=lambda r: key(r, a))
                    checks += 1
                fixtures += 1
    rows = [(2, 3, 2), (3, 6, 0), (4, 9, 1)]
    for maximum in range(1, 6):
        path, count = build(iter(rows), root, maximum)
        for a in range(1, maximum+1):
            assert lookup(path, count, a) == min(rows, key=lambda r: key(r, a))
    assert [lookup(path, count, a)[2] for a in range(1, 6)] == [2, 2, 0, 1, 1]
    first = 2**53+1
    rows = [(1, 1, 1), (2, first+2, 0)]
    path, count = build(iter(rows), root, first+1)
    assert lookup(path, count, first-1) == rows[0]
    assert lookup(path, count, first) == rows[1]
    assert int(float(first)) == first-1
    rows = [(1, 1, 0), (2, 7, 1), (3, 12, 2)]
    path, count = build(iter(rows), root, 20)
    assert count == 2
    for a in range(1, 21):
        assert lookup(path, count, a) == min(rows, key=lambda r: key(r, a))
    for p in (1, 2, 3, 8, 32, 128):
        rows = [(c, c*c, c) for c in range(1, p+1)]
        path, count = build(iter(rows), root, max(1, p*(p-1)+1))
        assert count == p
        for c in range(1, p+1):
            assert lookup(path, count, c*(c-1)+1) == (c, c*c, c)
    H = 2**63-1
    rows = [(H-1, H-1, 2), (H, H, 1)]
    path, count = build(iter(rows), root, H-1)
    for a in (1, H-2, H-1):
        assert lookup(path, count, a) == min(rows, key=lambda r: key(r, a))
    assert H*(2*H-1) < 2**127
    assert H*H+1 < 2**127
assert (fixtures, checks) == (7050, 126900)
print("exhaustive hull fixtures/checks", fixtures, checks)

sets = [frozenset(i for i in range(2) if mask >> i & 1) for mask in range(4)]
pools = [frozenset(i for i in range(3) if mask >> i & 1) for mask in range(8)]
queries = accepted = monotone = 0
for targets in product(sets, repeat=3):
    for P in sets:
        cs = [len(P & T) for T in targets]
        for S in sets[1:]:
            actual = [(-F(len(S & T), len(S | T)), i) for i, T in enumerate(targets)]
            counter = [(-F(cs[i], len(S)+len(T)-cs[i]), i) for i, T in enumerate(targets)]
            C = {i for i, T in enumerate(targets) if len(S & T) != cs[i]}
            for self_id in (None, 0, 1, 2):
                eligible = set(range(3)) - {self_id}
                for k in range(5):
                    kp = min(k, len(eligible))
                    want = sorted(eligible, key=lambda i: actual[i])[:kp]
                    passed = []
                    for U in pools:
                        O = set(range(3)) - U
                        W = sorted(U-C-{self_id}, key=lambda i: counter[i])
                        E = min((counter[i] for i in O), default=None)
                        gate = kp == 0 or not O or (len(W) >= kp and counter[W[kp-1]] < E)
                        counted = kp == 0 or not O or sum(counter[i] < E for i in W) >= kp
                        assert gate == counted
                        if gate:
                            assert sorted((U | C)-{self_id}, key=lambda i: actual[i])[:kp] == want
                            accepted += 1
                        passed.append(gate)
                        queries += 1
                    for i, U in enumerate(pools):
                        for j, V in enumerate(pools):
                            if U <= V and passed[i]:
                                assert passed[j]
                                monotone += 1
assert (queries, accepted, monotone) == (122880, 42732, 109536)
print("gate queries/acceptances/nested-U checks", queries, accepted, monotone)
print("tie-only, horizon, large-integer, skyline and Theta(p) fixtures passed")
```

Run from the repository root using the author's first Python fence and this review's third Python fence:

```sh
awk '/^```python$/{inside=1;next} inside && /^```$/{exit} inside{print}' research_algorithms_20260920/Similarity-Envelope-Certificate.md |
python3 -c "$(awk '/^```python$/{n++;next} n==3 && /^```$/{exit} n==3{print}' research_algorithms_20260920/Similarity-Embeddings-Independent-Review.md)"
```

Reviewed envelope source SHA-256: `63a0bb780ca4ee4aeb9c687942536c64a3413bad6976f68cb66e1892d08dc438`. Its newline-terminated Python fence SHA-256 is pinned in the probe. The pre-append review consisted of 48,275 bytes with SHA-256 `a28234058b22f6f888781660a7001d98a7ed6586aa51e5905e3062a6a507bba2`; those bytes are preserved. Only this append was authored for the new A05 follow-up.

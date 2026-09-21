# Similarity: Shared Query-Local Exact Execution

Date: 2026-09-21. Implementation plan and evidence record. Research prototype,
not a physical memory limit, new generic DP, or completed paper claim.

## Goal And Selected Design

Integrate the reviewed neutral/offset substitution into a polynomial inverse
solver and complete exact top-k queries. This follows the approved ongoing
research plan; no production API change, commit or push is part of this step.

Three options are full-core preparation reuse, query-local branching, and
query-local inverse evaluation. The third removes inactive state without
depending exponentially on visible conflicts. Retain the old full-core dual
as a semantic and work control, not merely the defeated global branch solver.
Keep the query-local branch probe unchanged as a second finite oracle.

One sparse query preparation constructs a binary plan used first by a cheap
interval relaxation and, only if needed, by the exact inverse evaluator.
The cheap relaxation includes inherited neutral E and is explicitly named
neutral-aware; it is NOT the old interval baseline under a new implementation.
The old frozen interval control remains a separate comparison.

## Boundary Contract

For a prepared component, obtain its true population from its original index
entry minus the populations of its active boundary children. Given its bundle
(lo,hi,debt,B), compute E=lo+hi-H and b=lo-debt. Base inverse thresholds are
E for the major role and E+max(0,c-b) for the minor role, for 0<=c<=B.
Validate E>=0, lo<=hi, 0<=b<=B and B-b<=hi-lo. Never infer H=lo+hi.

The old attained-maximum inverse recurrence then applies unchanged. Binary
additive merges have summed C,H and reward ceilings; strict merges use their
actual C. The plan retains actual root H and E. The interval relaxation uses
the same bases but drops visible attained-maximum disjunctions, retaining
their upper and complement size caps. Thus exact<=neutral-aware interval;
complete-pair witnesses still exist for the exact root answer.

For rq=0 the plan has a single offset base and its interval score is already
exact; no threshold arrays are needed. The distinction from global r=0 is
material: hidden inactive conflicts can force E>0.

## Test-First Steps

1. Add `experiments/test_query_local_similarity.py`; observe the missing-module
   assertion on the positive-E example before creating implementation code.
2. Add `experiments/probe_query_local_similarity.py`. Build only O(Ks) plan
   records with true populations. Evaluate neutral-aware intervals and exact
   inverse thresholds. Reserve threshold cells and recurrence work before
   allocating tables; preparation and control work remain separately charged.
3. Check 29,440 full-compatible-pair envelopes, 180 deeper full-core references,
   nonzero E, empty/invalid input, strict budget boundaries, scan-free scaling,
   and the single-offset exact shortcut. These reuse existing finite corpora,
   not newly discovered real workloads.
4. Wire a compact source view and sparse posting merge into full top-k. Preserve
   deterministic ID ties, self exclusion, zero fillers and exact fallback on
   resource/profile refusal. Share the plan between the cheap and exact stages.
5. Verify complete small outputs against the brute-force set oracle. Then run
   fixed public queries against full-core dual, old interval, and posting DAAT.
   Measure modeled work/state only here, not contaminated timings or RSS.
6. Update this evidence record and the resumable journal. Retain all unfavorable
   results and leave all seven original contribution obligations active.

## Accounting And Novelty Guardrails

The binary plan may exceed Ks because additive merges become explicit; count
its records separately. Threshold work/cell caps do not include source import,
static-index construction, posting traversal, sparse marking, plan construction,
interval evaluation, rational-bit costs, body fallback, or output. They are
not a complete-query RAM or deadline contract. Paid static arrays, resident
source bodies and both posting controls remain in the experimental accounting.

This tests whether the specialized substitution is useful in an actual query
schedule. Generic memoized DP with the same substitution is an equivalent
algorithmic control, not something this integration has proved inferior to.
No timing or RAM claim follows merely from fewer evaluated records.

## Derived Bounds And Their Scope

Let c be the number of component/capsule records and rq the number of strict
records in the reduced core, so Ks=c+rq. There are Ks-1 core edges, of which
2*rq leave strict records. Expanding each component's ordinary/neutral bundle
as one base and each outgoing edge as one binary additive merge produces

```text
N = rq + c + (Ks-1-2*rq) = 2*c-1 <= 2*Ks-1
```

binary-plan records. The empty-support neutral root gives N=1. No subtree is
duplicated by this expansion. This is why binary-plan peaks must not be
reported as if they were the original core peaks.

Let B be the sum of base reward ceilings, an integer bounded by query/union
overlap, not by the number of positive query leaves. Each plan node's ceiling
is the sum below it. Retaining both role tables therefore takes O(N*(B+1))
threshold cells. Leaf and strict-node work, plus the linear terms of additive
convolution, cost O(N*(B+1)). The additive quadratic terms are bounded by
O(B^2): a pair of reward units from different bases crosses the left/right
boundary only at their unique lowest common ancestor. Counting only additive
ancestors is no larger. The resulting exact-evaluation bound is

```text
prepared inverse work:  O(N*(B+1) + B^2)
retained table cells:   O(N*(B+1))
shared interval work:  O(N)
```

Add sparse construction, lookup, plan expansion and actual integer bit costs
separately. This is a query-local specialization of the already proved inverse
recurrence, not a new convolution or tree-knapsack theorem.

The neutral-aware interval is safe by relaxing only the visible strict
attainment disjunctions. Its bases preserve their exact optimal reward at each
size. Each resulting base state lifts to an old scored-row interval state,
so imposing neutral feasibility cannot improve on the old interval optimum.
The envelope ordering is exact <= neutral-aware interval <= old interval.
For rq=0 there is one base, hence no remaining disjunction to relax. This
explains the exact shortcut, including nonzero E; it is not inferred merely
from a lucky finite test.

The same many-conflict family from the proof manuscript was evaluated with
both inverse solvers, with identical exact scores:

| G | Local N | Local cells | Local work | Full-core cells | Full-core work |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 5 | 34 | 66 | 50 | 88 |
| 16 | 5 | 34 | 66 | 146 | 220 |
| 64 | 5 | 34 | 66 | 530 | 748 |
| 256 | 5 | 34 | 66 | 2,066 | 2,860 |

This demonstrates the declared local-dependence regime. It is engineered
evidence, not observed public peak reduction; cached generic DP can adopt the
same specialization.

## Additional Primary-Source Check

Inspected on 2026-09-21: [LES3: Learning-based Exact Set Similarity Search,
Section 3.1-3.2, PDF page 3 / printed page 2075](https://vldb.org/pvldb/vol14/p2073-li.pdf).
Its token-group bitmap records whether any member of a record group contains
a token. Equation (2) bounds Jaccard by query/group-union overlap divided by
query size; surviving groups undergo verification. Theorem 3.1 establishes
applicability to qualifying measures. This is direct precedent for grouped
union filtering and compact bitmap storage, not a newly discovered technique.
Those inspected sections do not establish our two-hidden-row attained-maxima
completion and nonzero-E substitution. That distinction is a limited source
comparison, not a priority finding. The full [earlier closest-art assessment](Similarity-Orientation-Prior-Art-Assessment.md)
also covers count-vector KNN, laminar optimization and strong-backdoor methods.

Also inspected: [Arasu et al., Efficient Exact Set-Similarity Joins,
Section 3, PDF page 3 / printed page 920](https://www.vldb.org/conf/2006/p918-arasu.pdf).
The signature/candidate/verification separation and intermediate-result cost
model reinforce why fewer final body candidates alone do not establish a
complete-query advantage. No benchmark number is imported from that paper.

## Frozen Public Study Protocol

Use G=64, original-ID row pairing and the same sixty query IDs as the frozen
larger-G receipt. The source adjacency membership count supplies an unclipped
tau profile. Five complete-query pipelines are compared: equally compressed
old interval, old full-core dual, neutral-aware control, shared local dual,
and target-posting DAAT. Full-core and local dual get the same 65,536 cell
and recurrence-work caps per call. The old driver's cumulative recurrence cap
is set to 10^12 and must remain nonbinding; the new runner does not yet expose
that aggregate cap. Source import, dense builder and all static arrays remain
resident in this study and are not excluded from a claimed RAM measurement:
there is no such measurement.

Every output is compared with both the prior frozen output and a direct set
oracle. Kernel-work totals can differ from two mechanisms: avoiding exact
calls with the stronger cheap control, and evaluating less state in the calls
that remain. Do not attribute the combined difference exclusively to one.
The old full-core pipeline still uses dense cheap preprocessing; the old
interval pipeline below is equally sparse, so that cost is exposed separately.

Run the retained listing from the repository root. It refuses to overwrite
its result and pins the older receipt and source hashes. Preparation metrics,
all output tuples, all per-query counters and source code hashes are retained.

```sh
awk '/^```python$/{emit=1;next} emit && /^```$/{exit} emit{print}' research_algorithms_20260920/Similarity-Query-Local-Execution.md | /Users/amuldotexe/.local/bin/python3.11 -B -W error -
```

```python
from collections import Counter
import gzip
import json
from pathlib import Path
import sys

root = Path('research_algorithms_20260920')
sys.path.insert(0, str(root / 'experiments'))
from profile_large_group_similarity import prepare_large_group_source, run_large_group_query, hash_similarity_input_file
from probe_sparse_control_similarity import prepare_compact_source_view, run_sparse_interval_query
from probe_query_local_similarity import prepare_skeleton_source_view, run_skeleton_similarity_query
from bench_bounded_seed_similarity import read_undirected_graph_source
from test_bounded_seed_similarity import compute_finite_similarity_oracle

prior_path = root / 'evidence/similarity-large-groups-20260921/receipt.json'
assert hash_similarity_input_file(prior_path) == 'bf50ab091409266c153f82291b9001a2bcd0c1ec447a9d0faf074397f8d0796a'
prior = json.loads(prior_path.read_text())
destination = root / 'evidence/similarity-query-local-20260921/receipt.json'
assert not destination.exists(), 'refuse overwrite of retained evidence'
names = set(prior['code_sha256']) | {
    'probe_query_local_similarity.py', 'test_query_local_similarity.py',
    'probe_query_skeleton_similarity.py', 'test_query_skeleton_similarity.py',
    'probe_sparse_control_similarity.py', 'probe_compressed_interval_similarity.py',
    'test_bounded_seed_similarity.py'}
hashes = {name: hash_similarity_input_file(root / 'experiments' / name) for name in sorted(names)}
assert all(hashes[name] == digest for name, digest in prior['code_sha256'].items())
receipt = dict(kind='resident complete-query logical-work study; not time or RSS',
    prior_sha256=hash_similarity_input_file(prior_path), inputs=prior['inputs'],
    groups=64, k=10, state_cap=65536, work_cap=65536, query_work_cap_control=10**12,
    code_sha256=hashes, preparation=[], runs=[])
frozen = {(row['dataset'], row['sid']): row['rows'] for row in prior['runs']
          if row['groups'] == 64 and row['mode'] == 'interval'}
for item in prior['inputs']:
    assert hash_similarity_input_file(Path(item['source'])) == item['sha256']
    with gzip.open(item['source'], 'rt', encoding='utf-8') as stream:
        graph, normalization = read_undirected_graph_source(stream)
    assert normalization == item['normalization']
    targets = sorted(graph.items())
    limit = sum(len(row) for _, row in targets)
    dense = prepare_large_group_source(targets, 64, query_limit=limit)
    sparse, local = prepare_compact_source_view(dense), prepare_skeleton_source_view(dense)
    indexes = [block['index'] for block in local['blocks'] if block['index'] is not None]
    receipt['preparation'].append(dict(dataset=item['dataset'], query_limit=limit,
        resident_builder=dense['preparation'],
        added_index_array_entries=3*sum(len(index['parents']) for index in indexes),
        added_owner_endpoints=sum(len(index['owner_ends']) for index in indexes)))
    totals = {}
    for sid in item['source_ids']:
        query = graph[sid]
        oracle = compute_finite_similarity_oracle(targets, query, sid, 10)
        assert [list(row) for row in oracle] == frozen[item['dataset'], sid]
        for mode in ('interval', 'full_dual', 'neutral', 'local_dual', 'posting_merge'):
            if mode == 'interval':
                rows, metrics = run_sparse_interval_query(sparse, query, sid, 10)
            elif mode in ('full_dual', 'posting_merge'):
                rows, metrics = run_large_group_query(dense, query, sid, 10,
                    'modal_dual' if mode == 'full_dual' else mode,
                    state_cap=65536, work_cap=65536, query_work_cap=10**12)
            else:
                rows, metrics = run_skeleton_similarity_query(local, query, sid, 10,
                    mode='dual' if mode == 'local_dual' else mode, state_cap=65536, work_cap=65536)
            assert rows == oracle, (item['dataset'], sid, mode)
            if mode in ('neutral', 'local_dual'):
                assert metrics['skeleton_attempts'] == metrics['skeleton_preparations']+metrics['skeleton_refusals']
                assert metrics['skeleton_preparations'] == metrics['control_evaluations']
                assert metrics['threshold_attempts'] == metrics['threshold_evaluations']+metrics['threshold_refusals']
                assert metrics['operations'] <= metrics['reserved_operations']
            receipt['runs'].append(dict(dataset=item['dataset'], sid=sid, mode=mode,
                                        rows=rows, metrics=dict(metrics)))
            aggregate = totals.setdefault(mode, Counter())
            for key, value in metrics.items():
                if key.endswith('_peak'):
                    aggregate[key] = max(aggregate[key], value)
                else:
                    aggregate[key] += value
    print(json.dumps(dict(dataset=item['dataset'], totals=totals), sort_keys=True), flush=True)
assert len(receipt['runs']) == 300
assert hashes == {name: hash_similarity_input_file(root / 'experiments' / name) for name in hashes}
receipt['complete_results_checked'] = len(receipt['runs'])
destination.parent.mkdir(parents=True, exist_ok=True)
with destination.open('x', encoding='utf-8') as stream:
    json.dump(receipt, stream, sort_keys=True, indent=2)
    stream.write('\n')
print('receipt', destination, flush=True)
```

## Executed Results

The explicit missing-module assertion was observed before implementation.
Seven focused tests pass. The complete similarity suite then passed **145
tests**, warnings as errors, terminal exit zero. Its 25.391 seconds is test
cost, not performance data. The public process completed with exit zero and
**300 complete outputs** matching both the frozen results and the direct set
oracle. The same sixty query IDs are repeated across five modes, not 300
independent workloads. No profile, skeleton or threshold refusals occurred
in these public runs; separate tests exercise refusals and exact fallback.

| G=64, sum over thirty queries per dataset unless marked peak | GrQc | Facebook |
| --- | ---: | ---: |
| Old sparse interval: modeled body memberships | 11,940 | 206,686 |
| Neutral-aware control: modeled body memberships | 11,940 | 206,686 |
| Whole-core dual: modeled body memberships | 11,447 | 198,213 |
| Query-local dual: modeled body memberships | 11,447 | 198,213 |
| Old sparse interval: control record visits | 3,663 | 35,525 |
| Local control: binary-plan record visits | 2,384 | 26,943 |
| Whole-core dual: exact recurrence evaluations | 322 | 1,643 |
| Local dual: exact recurrence evaluations | 241 | 1,199 |
| Local rq=0 exact-offset shortcuts | 81 | 444 |
| Whole-core dual: counted recurrence steps | 48,410 | 1,779,314 |
| Local dual: counted recurrence steps | 40,136 | 1,661,350 |
| Whole-core dual: peak threshold cells in one call | 814 | 3,654 |
| Local dual: peak threshold cells in one call | 814 | 3,654 |
| Old sparse interval: largest individual control table | 31 | 65 |
| Local control: largest individual binary plan | 31 | 83 |
| Block-posting memberships read by both local modes | 7,237 | 155,786 |
| Target-posting DAAT memberships read | 7,260 | 175,952 |
| Target-posting DAAT target-body memberships read | 0 | 0 |

The local exact stage uses **17.09% / 6.63% fewer recurrence steps** than this
study's whole-core dual, in the same counter units. Removing zero-conflict
exact calls and reducing the remaining plans both contribute; this is not a
clean measurement of either mechanism alone. The stronger neutral control
produces **no additional public pruning** over the old interval, despite the
positive-E counterexample establishing that it can be strictly stronger.
All 81/444 avoided exact calls on these runs are accounted for by the offset
shortcut. Complete-query wall time is unmeasured and could still worsen.

The older larger-G receipt used tau=8; this study uses unclipped tau, as did
the subsequent sparse-control study. Thus its full-dual Facebook recurrence
total is 1,779,314, not the older 1,682,868. Do not compute a cross-profile
speed/work ratio by quietly switching denominators. Both controls here use
the identical new source/profile and per-call budgets. The old cumulative
cap is nonbinding.

Peak threshold cells do not improve at all. The local control's Facebook
peak grows from 65 ordinary core records to 83 explicit binary-plan records.
These records have different fields and are not byte equivalents. The
reduced core, binary plan, and evaluator state also coexist; a threshold cell
count alone is not full transient memory. Static overhead additionally pays
50,106 / 112,521 entries across the three new arrays and 13,374 / 28,871 owner
endpoints. Source bodies, union postings, comparison target postings, imported
graph objects and dense builder data remain resident in this experiment.

DAAT's 7,260 / 175,952 target-posting visits cannot be added or ratioed directly
against recurrence steps or body members to manufacture a time claim. It
needs no target-body reads and remains the mandatory strong alternative.
Earlier unfavorable complete-query timing evidence is not reversed here.

Receipt: [all output rows and counters](evidence/similarity-query-local-20260921/receipt.json),
SHA-256 `fe67f823935098a7908ebfa6d1ca15279596ad81f8769e02bfaa76d3d18cd648`.
The receipt pins the old receipt, both source archives and all relevant code
hashes before and after the run. New module and test hashes are respectively
`a03c2fe681db2f0dcc496df1213bd56ec9d1d497fcfdd2474e442fc467ca7b8d` and
`0fc3d20c72a24d3ba004cb9b1869eda3417e3453b5f30c9e711e72cd1f90e631`.

## Decision After Rubber-Duck Checks

- Does a smaller pre-filter skeleton imply lower peak query RAM? No: the
  executed maxima are unchanged and static arrays add cost.
- Does the new exact method prune more than the old exact method? No: their
  answers and body decisions match on this admitted profile, as intended.
- Is preparation shared? Yes: 540 / 2,163 skeletons and binary plans serve
  the cheap control and all surviving exact calls, without second query
  preparation. Index preparation, raw posting work and body fallback remain.
- Is rq=0 an excuse to use the old interval? No: inactive E may be positive.
  The exact shortcut must evaluate the offset base with its actual population.
- Is this a faster or low-RAM product yet? No. It closes a real query-local
  implementation gate and demonstrates a logical-work improvement. It does
  not overcome the current public workload's modest extra body pruning.

The next A05 gate is a paid static-versus-query tradeoff on a genuinely useful
workload where sparse local dependence matters, or a precise closest-art
argument supporting the specialized theorem as a theoretical contribution.
Do not immediately fund packed full ingestion just because the pre-filter
opportunity ratios looked large. An equally equipped memoized tree DP shares
these bounds. The bounded independent code/accounting review is now completed,
lead-replayed and closed; its findings and the resulting repair follow.

## Independent Audit And Lifetime Repair

The [code/accounting review](Similarity-Query-Local-Code-Review.md) found no
answer/pruning counterexample in its bounded independent checks. The lead
read and replayed the full retained checker: 226 complete-compatible-envelope
comparisons, 256 seeded top-k comparisons, eight tie/zero/self cases, four
fallback paths, and exact state/work/skeleton boundary tests all pass. These
are distinct from the earlier mathematical review and from the lead suite.

It also exposed two actual mistakes. First, the previous block's local plan
was still referenced while the next plan was built. A witness had six live
plan records while `plan_records_peak` reported the largest single plan of
three. Second, the earlier mathematical review's illustrative second row
`{b,e}` failed its own stated leaf maximum; `{d,e}` is the correct row. The
example has been corrected with an explicit erratum. Its direct-metadata
theorem/checker were unaffected.

The lifetime defect was reproduced by a new regression: **RED, 6 != 3**.
The runner now clears the previous core and plan at the start of each block.
All eight focused tests then pass, including the lifetime regression. This
repairs overlapping plan ownership, not the complete RAM-accounting problem.
Posting generators still number all query features, while `posting_streams`
counts only nonempty streams. Caps remain per attempted block, not total
query work; three 44-step attempts can total 132 steps. These limitations
are explicit and were already permitted by the public protocol.

The initial 300-output receipt is immutable. Exact pre-fix module/test bytes
are archived beside it under their original filenames and match its hashes.
The independent review's replay selects that archive to preserve its original
failure witness. The live-source regression verifies the repair separately.
For reconstructing the initial full study in a fresh output directory, use
the archived module as the leading import path and the archived two source
files when collecting hashes. Other dependencies are unchanged.

The following post-fix replay checks every affected public output and every
reported counter against the original receipt, without rerunning unaffected
controls or making new timing claims. It saves a separate validation receipt.

```sh
awk '/^```python$/{n++;emit=(n==2);next} emit && /^```$/{exit} emit{print}' research_algorithms_20260920/Similarity-Query-Local-Execution.md | /Users/amuldotexe/.local/bin/python3.11 -B -W error -
```

```python
import gzip
import json
from pathlib import Path
import sys

root = Path('research_algorithms_20260920')
sys.path.insert(0, str(root / 'experiments'))
from profile_large_group_similarity import prepare_large_group_source, hash_similarity_input_file
from probe_query_local_similarity import prepare_skeleton_source_view, run_skeleton_similarity_query
from bench_bounded_seed_similarity import read_undirected_graph_source

base = root / 'evidence/similarity-query-local-20260921'
prior_path = base / 'receipt.json'
assert hash_similarity_input_file(prior_path) == 'fe67f823935098a7908ebfa6d1ca15279596ad81f8769e02bfaa76d3d18cd648'
prior = json.loads(prior_path.read_text())
changed = ('probe_query_local_similarity.py', 'test_query_local_similarity.py')
for name, digest in prior['code_sha256'].items():
    path = base / name if name in changed else root / 'experiments' / name
    assert hash_similarity_input_file(path) == digest
current = {name: hash_similarity_input_file(root / 'experiments' / name) for name in changed}
destination = base / 'lifetime-fix-validation.json'
assert not destination.exists(), 'refuse validation overwrite'
checked = []
for item in prior['inputs']:
    assert hash_similarity_input_file(Path(item['source'])) == item['sha256']
    with gzip.open(item['source'], 'rt', encoding='utf-8') as stream:
        graph, normalization = read_undirected_graph_source(stream)
    assert normalization == item['normalization']
    dense = prepare_large_group_source(sorted(graph.items()), 64,
        query_limit=sum(len(row) for row in graph.values()))
    source = prepare_skeleton_source_view(dense)
    for record in prior['runs']:
        if record['dataset'] != item['dataset'] or record['mode'] not in ('neutral', 'local_dual'):
            continue
        rows, stats = run_skeleton_similarity_query(source, graph[record['sid']], record['sid'], 10,
            mode='neutral' if record['mode']=='neutral' else 'dual', state_cap=65536, work_cap=65536)
        assert [list(row) for row in rows] == record['rows']
        assert dict(stats) == record['metrics'], (item['dataset'], record['sid'], record['mode'])
        checked.append([item['dataset'], record['sid'], record['mode']])
assert len(checked) == 120
assert current == {name: hash_similarity_input_file(root / 'experiments' / name) for name in changed}
validation = dict(kind='post-lifetime-fix exact output and all-counter equality',
    parent_receipt_sha256=hash_similarity_input_file(prior_path), code_sha256=current,
    complete_results_checked=len(checked), checked_queries=checked)
with destination.open('x', encoding='utf-8') as stream:
    json.dump(validation, stream, sort_keys=True, indent=2)
    stream.write('\n')
print('PASS: 120 post-fix public outputs and all counters match; archived code hashes verified')
print('validation', destination)
```

Post-fix replay completed with terminal exit zero: all **120 affected public
outputs and every reported counter** match the initial receipt. The final
full similarity suite passes **146 tests** with warnings as errors, terminal
exit zero (25.528 seconds of test cost). No code changed after these runs.
This is a repair/parity run over the same sixty query IDs, not new independent
workload evidence. Both reviewers are completed and closed; all associated
execution sessions are terminal.

Validation receipt: [lifetime-fix-validation.json](evidence/similarity-query-local-20260921/lifetime-fix-validation.json),
SHA-256 `0ebda35afdf462e8947f65453d404ad4f9255172081380360a37ca5bd2113874`.
Current module/test hashes are
`9e314f5630c7c24748a4077c5a9f0260904aec671c6736aec30d2d00064cf7ff` and
`f9e6a418fb1531fa673f08b113a5754d0c799e58f999a33c6db29739ef66dd03`.

The implementation-plan steps above are complete for this bounded research
stage. The full seven-family objective is not. Next preserve this A05 result
as a conditional specialized reduction and require useful source/closest-art
evidence before expanding its physical implementation. The A04 broader
monotone-execution class, A02 complete native comparison, and A07 compact
multiway alternative remain substantive portfolio directions; neither this
integration nor a new name for known DP closes their contribution gaps.

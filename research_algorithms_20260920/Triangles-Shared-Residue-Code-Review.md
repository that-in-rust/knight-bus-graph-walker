# Independent Shared-Residue Code And Resource Review

Date: 2026-09-20. Bounded implementation review, separate from the earlier mathematical/prior-art review and the active seven-family research goal.

**Lead resolution after review:** F1 below was reproduced in twelve regression subcases and fixed by validating actual period-map key components before equality matching. The combined suite then passed all 51 tests. The reviewed source and all reviewer observations below are preserved as historical evidence; the reviewer did not re-review the fix. Post-fix source SHA-256: `0700a97cf86ac00c7cd64facf5824fb93df13f5fb9c75782b5f9d942384c1baf`.

## Findings First

### F1 [P3]: Period-Map Keys Bypass Integer-Only Source Validation

Location: `experiments/probe_shared_residue_triangles.py:114`, with the unchecked lookup at line 118 and class-key-only validation at line 119.

`normalize_native_triangle_source` compares the two mappings' key sets, then validates only the key components obtained from `classes`. Python key equality does not enforce integer types: `(False, True, False)` and `(0.0, 1.0, 0.0)` both compare equal to `(0, 1, 0)`. Consequently non-integer keys in `mask_periods` pass both key matching and the later integer check. The latter checks the canonical key from the other mapping, not the supplied period-map key.

This is a narrow mismatch with the Composition document's Graph-Level Correctness statement that canonical class keys and integer types are validated before processing. It is not evidence of wrong counts for canonical integer inputs; the accepted aliases produce the same graph. Severity is low because the demonstrated effect is inconsistent source rejection, not numerical corruption.

Executed reproduction, using Python 3.11 with the experiments directory on `sys.path`:

```python
from probe_shared_residue_triangles import (
    compute_local_period_triangles,
    iterate_local_triangle_rows,
)

for period_key in ((False, True, False), (0.0, 1.0, 0.0)):
    source, total, metrics = compute_local_period_triangles(
        2, 2,
        {(0, 1, 0): [(0, 2)]},
        {period_key: 2},
    )
    assert source["edge_count"] == 2
    assert list(iterate_local_triangle_rows(source, 1)) == [(1, 0)] * 4
```

Observed: both calls succeed. Under a uniformly integer-only source-key contract, both should raise `ValueError`. The same non-integer components supplied as actual `classes` keys are rejected.

Suggested correction: validate the actual keys of both mappings before equality/lookup, applying the same scalar-type rule. Alternatively, explicitly document that only `classes` keys must be canonical and that Python-equal numeric aliases are accepted in the auxiliary period map. No correction was made in this sidecar.

**No additional correctness or stated logical-resource-contract findings were found in the inspected scope.** In particular, no wrong global total, local row, degree, displacement closure, wraparound, or event-admission result was observed in the checks below.

## Inspected Scope

- All 211 lines of `experiments/probe_shared_residue_triangles.py`: histogram construction, lazy event streams, scalar admission/counting, native source normalization, class-term traversal, global aggregation, predicate membership, and complete-row export.
- Only the four imported helpers in `experiments/probe_masked_matching_triangles.py` were assessed: `require_integer_scalar_values` (line 6), `validate_mask_interval_runs` (line 11), `shift_mask_interval_runs` (line 31), and `enumerate_oriented_shift_pairs` (line 135). Other masked/hybrid implementations were not reviewed.
- Read `Triangles-Shared-Residue-Composition.md` and `Triangles-Shared-Residue-Review.md` as the contract/math references. Their cited external sources were not researched anew.
- Discovery started with `codebase-memory-mcp` on indexed project `Users-amuldotexe-Desktop-personal-repos-lane-knight-bus-graph-walker`, followed by symbol search and outbound tracing. An initial file-pattern query returned no matches; name-based graph search found the symbols. Source reads verified the actual calls rather than trusting unrelated graph resolutions of common built-in names.
- The new same-source elementary arithmetic comparator was not inspected or executed. Existing tests were neither edited nor rerun; the stated 16/41 passing suite counts are prior evidence, not this review's test receipt.

## Fresh Executed Evidence

All probes ran synchronously via `/Users/amuldotexe/.local/bin/python3.11 -B -`, with in-memory scripts and standard-library oracles. No probe file or bytecode cache was written by these runs. Expected graph answers came from independently expanded undirected adjacency and unordered neighbor-pair tests, not another project counting implementation.

| Check | Actual cases | Observed result |
| --- | ---: | --- |
| Closure helper: every triple of displacement subsets for each `L=1,2,3,4`, including empty subsets | 4,680 | Exact closure sets, no duplicate triples, and exact cheapest-pair probe counts |
| Shift helper: every Boolean subset for `q=1..6`, every displacement in `[-2q,2q]`, plus `+/-(10^100+7)`; reversed singleton runs exercise normalization | 2,946 | Exact translated residue sets and canonical nonadjacent output runs |
| Scalar counts and event metadata against direct root scans and independently constructed dense histograms | 192 | Exact counts, planned/processed events, raw/shared cyclic counts, and `K_shared <= K_raw` |
| One-event-below-budget calls for the positive-event scalar cases | 121 | All refused before stream creation; an in-memory mock made any premature stream call fail |
| Graph-wide cumulative admission fixture with two disconnected closing class triples | 1 fixture | Total 6, event cost 10; budget 10 accepted, budget 9 refused |
| Every graph on three bases with `L=2`, using all 12 possible cross-base edges; fourth base always isolated | 4,096 snapshots | Exact global/edge totals and all rows; 8,192 complete exports at block sizes 1 and 3; 65,536 rows |
| Mixed native-period graphs with shuffled class order and an isolated final base | 36 snapshots | 108 complete exports at block sizes 1, 5, and `L+1`; 9,036 exact rows |
| Wholly isolated graphs, including an explicitly empty class | 3 snapshots | 9 complete exports; 429 exact zero rows |
| Non-integer scalar fields across public count/source/output boundaries and shift/histogram helpers | 142 | All rejected with `ValueError` |
| Invalid bounds, arity, overlap, missing/extra period keys, and empty-class validation | 25 | All rejected with `ValueError` |
| Non-integer period-map key aliases | 2 | Both accepted; reproduction of F1 |
| Large-integer count/prefix/admission checks | 3 | Results described below; no full symbolic output claim |

The complete graph-oracle workload totals **4,135 snapshots, 8,309 complete exports, and 75,001 checked rows**. Every export was fully exhausted, including the iterator's final conservation assertions. The three source-index cardinalities were unchanged by every such export. This is finite correctness evidence, not a formal proof or an RSS benchmark.

Reproducibility parameters for the nonexhaustive cases:

- Scalar probes used `random.Random(920311)`, 32 trials for each ordered period tuple `(4,6,8)`, `(4,6,9)`, `(6,10,15)`, `(2,8,12)`, `(5,7,11)`, `(6,6,6)`. Each mask independently selected residues via `randrange(2)`. Trials 0, 1, and 2 forced the corresponding mask empty; trial 3 forced all full. `L=Q*(1+trial%3)`. There were 37 empty-mask shortcuts, 34 nonempty zero-event cases, and 121 positive-event cases.
- The nonempty closure cases exercised join choices `(0,1)` 1,875 times, `(0,2)` 1,112 times, and `(1,2)` 759 times. Empty-group cases explain the difference from the 4,680 total.
- Mixed graph probes used `random.Random(920312)`, cyclic lengths `(6,12,18,20,24,30)`, and `bases=3+trial%4`. Only the first `bases-1` bases had classes. Potential class keys were selected with `randrange(5)==0`, shuffled, then assigned a randomly chosen divisor of `L`; residues were selected in reverse order with nonzero `randrange(3)`.
- Wholly isolated fixtures were `(bases,L)=(1,1)`, `(7,11)`, and `(5,13)`. The last supplied empty class `(0,4,12)` with native period 13. Blocks were 1, 4, and `L+1`.
- Scalar type probes used `False`, `True`, `0.0`, `1.0`, `"1"`, `None`, `Fraction(1,1)`, and an `int` subclass. `None` was excluded only for optional event budgets, where it is explicitly valid.

## Contract And Resource Assessment

**Integer and Boolean masks.** Apart from F1, the inspected entry points reject non-plain integers, invalid divisors/dimensions, overlapping or empty-width runs, noncanonical class displacements, and invalid output blocks. Adjacent disjoint runs are merged, including after a shift. Empty classes still undergo validation before being discarded. The standalone stream/membership helpers rely on the validated internal representations supplied by their callers; arbitrary malformed internal dictionaries are not an advertised source contract.

**Closure and coordinates.** The three possible pair joins use the correct sum/difference modulo `L`, without duplicate class triples. A base triangle is visited only from its smallest two bases. Scalar counting rotates the middle predicate by `-first_shift`; export evaluates root `u`, `u-first_shift`, or `u-closing_shift` according to the vertex's base. Degrees use the class once at each endpoint, independently of triangle enumeration. Because native periods divide `L`, negative intermediate coordinates and graph wraparound agree under native modulo reduction.

**Event admission.** Histogram coalescing preserves multiplicities and zero-count spans. Internal boundaries and nonzero wrap boundaries are counted separately, with initial zero-position jumps omitted. Simultaneous stream events create zero-length intermediate segments rather than incorrect area. Empty-mask shortcuts require zero processed events even though diagnostic cyclic counts can remain nonzero. Primitive refusal occurs before stream creation; graph admission subtracts already processed events across successive class triples. This is the advertised event budget, not a preparation, class-join, export, time, or byte budget.

**Retained state.** Source normalization retains class/run indexes, not vertex-expanded state. Each scalar evaluation retains normalized masks, compact histogram/event state, and at most one pending heap item per stream. The wrapper consumes one class triple at a time and does not retain a term list. Export has two arrays of at most `min(B,L)` entries, with only constant-factor transient overlap when moving between blocks; it regenerates terms and scans classes as disclosed. These are source-level logical-state observations, not measured process-memory guarantees. Arbitrary-precision integer widths remain paid.

The large-integer checks were deliberately bounded:

1. At `L=2^256`, three full-mask classes with displacements `L-1`, `L-1`, and closing displacement `L-2` returned exactly `T=L`, `E=3L`, zero histogram events, and three histogram runs. The first eight rows at block size 3 were `(2,1)`; the iterator was then explicitly closed.
2. An all-isolated universe with `bases=2^256`, `L=1` produced eight `(0,0)` prefix rows without populating any source index; the iterator was explicitly closed.
3. With `N=2^256`, periods `(2,2N,2N)` and masks `[0,1)`, `[0,N)`, `[0,N)`, a zero budget rejected the exact planned `2N+1` events before any stream call. The huge sweep was not run.

These prefix checks do not exercise full-stream conservation on symbolic graphs. The 4,135 finite graph snapshots do. The documented absence of an RSS cap, symbolic source/import limitations, potentially exponential event work, slow regenerated output, and checks performed only after full iterator consumption are **not new bugs**.

## Next Meaningful Risk

After resolving or clarifying F1, the next useful evidence is an end-to-end complete-output run on a realistically sized, high-class-fanout source, measuring preparation, class joining, regeneration, delivery cost, integer storage, and peak/transient memory separately. A small histogram event count does not bound class-pair work or export rescans. Those costs are already disclosed, but remain the principal unmeasured implementation risk. Snapshot mutation/recovery and production importer eligibility are outside this executor's stated scope.

The separately developed comparator still needs its own review. This sidecar makes no judgment on its correctness, relative performance, or novelty, and does not close the seven-family goal.

## Reviewed Source Identity And Write Boundary

SHA-256 fingerprints of the inspected files, rechecked unchanged after writing this report:

```text
843d7165c486b172f67068d90318a5bcc456d7d5a5b8cbdaa955c89f8e25b3e9  experiments/probe_shared_residue_triangles.py
b2640cb46a9e04a8ac7700c645c0e16de0108fb8e414cd7c2a333c0bcbef65a0  experiments/probe_masked_matching_triangles.py
5e9f4b32ff71c6d6c13da50038dbe77be1800c7fa49cf00448dbf6380fd66394  Triangles-Shared-Residue-Composition.md
37a003ef08c391a4fe63bad03e62c2d3aa5f917a8d083781bab31b39ab2655c6  Triangles-Shared-Residue-Review.md
```

Only `research_algorithms_20260920/Triangles-Shared-Residue-Code-Review.md` was written by this sidecar. No experiment source, tests, shared contract/math document, or comparator was edited. No commit, push, background process, or external prior-art research was performed.

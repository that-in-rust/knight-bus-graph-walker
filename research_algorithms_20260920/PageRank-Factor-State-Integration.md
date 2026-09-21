# PageRank Factor-State Integration

## Scope

`research_algorithms_20260920/experiments/factor_rank_state_solver.py` extracts
the ordinary `cg` path from the frozen `probe_native_incidence_pagerank.py`.
It returns the actual final factor vector, not a vector reconstructed from
published scores. This is an integration/ownership change, not a new CG method,
convergence result, certificate, or solver novelty claim. No public timing run
or timing comparison is reported here. No integration driver is changed here.

## Interface

```python
solve_incidence_factor_state(
    path,
    source,
    epsilon,
    maximum_iterations=20000,
    maximum_factor_bytes=2**30,
) -> (h, metrics)
```

- `path`: existing `KBINC001` incidence row store, read-only throughout this call.
- `source`: `None` for uniform personalization, or a built-in integer in `[0, N)`.
  Booleans and floating-point indices are refused.
- `epsilon`: finite positive built-in float or integer representable as a finite
  float. No hidden solver-specific tightening: the caller's epsilon is retained;
  only the inherited internal `epsilon/8` screen is used.
- `maximum_iterations`: nonnegative built-in integer; booleans are refused.
- `maximum_factor_bytes`: nonnegative built-in integer; admits `6 * 8 * F`
  selected factor-array payload bytes. The exact boundary is accepted.
- `h`: a fresh owned mutable `array('d')`, length `F`, containing the actual final
  CG state. No score file, mapping pass, serialized state file, certificate, or
  hidden cache is created. Different calls do not share their arrays.
- `metrics`: scalar-only dictionary. It holds no arrays, views, closures, or
  deferred computation referencing solver state.

The helper validates finite epsilon, integer budgets, the header, source index,
and selected payload admission before allocating any factor vector or scanning
rows. `F=0` is supported, including a zero payload budget: return an empty
`array('d')`, zero iterations, zero screen, and the empty-byte SHA-256.

## Required Preconditions

**The caller must validate exact degrees/factor cardinalities upstream and keep
the source immutable throughout solving, reconstruction, and certification.**
In particular, singleton/empty factors must be pruned and each stored degree
must equal the sum of retained incident factor cardinalities minus one.
The inherited row decoder checks structure, ordering, truncation, and declared
membership counts; it does not establish those mathematical degree identities.

This helper does not build/verify a source manifest, rehash the source, pin an
open immutable file descriptor across scans, prevent replacement of the path,
or detect concurrent mutation. A caller must bind the query, source identity,
validation result, state, and eventual certificate. The state hash alone does
not bind the source or prove correctness. Upstream validation/identity work is
outside this helper's counters and timer.

## Faithful Arithmetic

Use the frozen float `ALPHA = 17/20`, row and sorted-factor order, and the same
floating-point expressions. The initial state is accumulated as `h[j] += p/d`,
not zero and not the RHS. Each RHS entry accumulates `p * (1/(d + ALPHA*q))`
before multiplication by `bscale = (1-ALPHA)/(1-ALPHA*pz)`. The operator copies
its input and subtracts `ALPHA*fsum(vector[j] for j in ids)/(d+ALPHA*q)` once
per incident factor, in the original order. Step and direction updates retain
the frozen expression ordering, including `next_squared/squared` per entry.

The residual array is pre-sized then filled with the same `rhs[j]-product[j]`
expression. This changes allocation mechanics, not the numerical operations.
No reassociation, preconditioner, residual replacement, normalization, clipping,
mass repair, or new CG technique is introduced.

The inherited recursive screen is
`ALPHA/(1-ALPHA) * fsum(counts[j]*abs(residual[j])) <= epsilon/8`.
It is **not a certificate**, including when it evaluates to exactly zero.
A tested two-vertex single-factor uniform query at `epsilon=1e-30` passes with
screen zero but its reconstructed binary64 output fails the frozen original-
operator certificate. The final publisher must certify its actual output bytes
at the requested epsilon before exposing an accepted answer. Returning `h`
does not mean that publication will succeed.

For bit-identical comparison, reconstruction uses this exact original formula:

```python
b = bscale * base.select_personalization_node_value(i, n, source)
value = degree*(b+base.ALPHA*math.fsum(h[j] for j in ids))/(degree+base.ALPHA*len(ids)) if degree else b
output.write(base.DOUBLE.pack(value))
```

Other publisher rounding paths may produce different bytes; certify the bytes
they actually produce rather than reusing a certificate for this formula.

## Failure and Ownership

Invalid/nonfinite parameters, insufficient factor payload budget, malformed
rows, bad/nonfinite curvature, nonfinite work, residual-norm underflow, and
iteration exhaustion refuse the state, normally with `ValueError`. Filesystem
errors, allocation failures, and arithmetic exceptions can also propagate.
There is no fallback answer or success/certificate flag on these failures.

**Intentional failure-path deviation from the frozen probe:** the exporter
checks the nonconverged iteration limit before starting the next CG step. With
limit zero it still prepares the initial state and applies the initial operator
to evaluate the screen, but performs zero CG updates. The frozen loop would
perform an extra unscreened update at the limit and then raise. Negative or
noninteger budgets are refused before allocation. Successful states, screens,
and iteration counts retain the frozen behavior, including convergence exactly
at the admitted limit.

A fresh private helper frame owns counts, RHS, product, residual, and direction.
That frame ends before the public wrapper hashes or returns `h`. Hashing packs
one value at a time as little-endian binary64, never copying the full state.
On refusal or cancellation, unwound traceback frames are cleared and the public
wrapper releases its `h` reference. Thus retaining an exception does not itself
retain the solver's arrays. `KeyboardInterrupt`/`SystemExit` are not swallowed;
row generators are explicitly closed during unwinding. Debuggers/profilers or
external callbacks that deliberately retain independent references are outside
this ownership guarantee. Cleared traceback locals are not available for
postmortem array inspection.

## Metrics and Memory

| Field | Meaning on successful return |
| --- | --- |
| `method`, `source_zero_based`, `vertices`, `factors`, `memberships`, `epsilon` | Query identity/dimensions; method is `cg` |
| `iterations_or_checks` | Number of completed CG updates, zero for an initially passing screen |
| `screen_bound_uncertified`, `screen_target_uncertified` | Recursive float screen and `epsilon/8`; not certified bounds |
| `isolate_count`, `source_isolate`, `bscale` | Frozen dangling-personalization preparation values |
| `state_sha256`, `state_bytes` | SHA-256 of sequential little-endian `<d` values, and `8*F` |
| `row_scans`, `noncertificate_row_scans` | Equal aliases; preparation + initial operator + one per update, thus `iterations+2` |
| `noncertificate_membership_visits` | Decoded membership occurrences: `row_scans*Z`, matching the frozen scan convention |
| `operator_membership_visits` | Gather plus scatter visits for operator calls: `2*(row_scans-1)*Z` |
| `logical_read_bytes` | `HEADER.size + row_scans*store_bytes`: admission header plus each decoder's separate header read and complete row body |
| `selected_array_phase_peak_bytes` | Conservative `6*8*F` selected logical payload admission, not measured RAM |
| `total_ms` | `perf_counter` wall interval from public entry through validation, allocation, preparation, CG, work-array release, and state hashing; excludes caller work |

`logical_read_bytes` does not claim physical disk traffic or cold-cache I/O.
Metadata `stat` operations and EOF probes returning zero bytes add no payload.
Scans do not include score reconstruction, publication, certification, or any
score-to-state mapping. Failures return no partial metrics.

The six simultaneous selected arrays are `h`, `counts`, `rhs`, `residual`,
`direction`, and the current operator `product`. Preparation holds three;
initial residual construction holds five; a CG step holds six. The conservative
six-array reservation also applies to early convergence. Only `h` survives
return. The selected payload excludes array headers/capacity, constructor seed
temporaries, row bytes/decoded tuples, generators, scalar objects, `fsum`
workspace, file buffers, allocator behavior, caller storage, and process RSS.
**Neither admission nor these tests prove whole-RAM fit.** No entity-sized score
array is allocated by this helper, but the inherited decoder is still row-sized.

## Integration Boundary

The earlier driver paid for CG output reconstruction/certification and then
mapped scores back to factors via `h[j] += score/degree`. That mapping can change
the state bits. It is eliminated **only when the caller actually uses the `h`
returned here**; adding this helper alone changes no existing driver's costs.
The new integration driver can pass the same epsilon to all five methods, take
ownership of this state, and perform its own final-byte certification. Any state
serialization/reload, publisher allocation, and upstream validation remain
separate paid work and must be accounted for there.

## Verification Evidence

Tests were written first: the initial run had 12 assertion failures because the
exporter did not exist. The later lead-interface tests separately failed on
missing `total_ms` and excess scans at exhausted limits 0, 1, and 2 before those
changes were made. No frozen source edit or commit was made.

The focused suite checks all 128 subsets of the seven nonempty subsets of three
vertices, for uniform and all three point sources: 512 complete tiny cases.
It captures the frozen function's actual final `h` using a temporary profiling
hook, compares every state byte, reconstructs every output byte with the original
float formula, checks SHA-256/screens/iterations/scans, and compares to the exact
expanded-operator `Fraction` oracle. Additional tests cover overlap/duplicates,
isolates, `F=0`, initial-state identity, budget boundaries, malformed inputs,
nonfinite values, curvature, pre-step exhaustion, cancellation, and ownership.

Run from the repository root (Python 3.11+; frozen all-method tests need NumPy):

```sh
PYTHONDONTWRITEBYTECODE=1 python3.11 -m unittest discover -s research_algorithms_20260920/experiments -p test_factor_rank_state_solver.py -v
PYTHONDONTWRITEBYTECODE=1 /Users/amuldotexe/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m unittest discover -s research_algorithms_20260920/experiments -p test_native_incidence_pagerank.py -v
LC_ALL=C shasum -a 256 research_algorithms_20260920/experiments/probe_native_incidence_pagerank.py
```

The system `python3` lacks `hashlib.file_digest`; Python 3.11.15 runs the focused
suite. The bundled Python 3.12.14/NumPy 2.3.5 runtime runs the frozen suite.
No dependency installation or compatibility change to frozen code was needed.

Frozen probe SHA-256, recorded before edits and checked after:
`d09558f63a906ccfdb859f7db6272caf1110474182b96fa7de088dfc3db64b65`.

Pinned fixture: `N=5`, groups `[[0,1,2],[0,1],[0,1],[1,3],[4],[]]`,
`epsilon=1e-10`. State/output hashes agree with the frozen CG path:

| Source | Iterations | Actual-state SHA-256 | Complete-output SHA-256 |
| --- | --- | --- | --- |
| Uniform | 3 | `8fd6732fc8071ccc42266fb96fbe6c95d8072896db3d6b019e0caac9b5c3b295` | `540f98241019b541ea63a79b471f2eff21781e109d708a830991b8eefc916618` |
| 0 | 3 | `ea225c6b08d6a5da8bce49468d082cc45c41efad89140b502a8db3796816d153` | `d64dbd0f41ade5f652872cd89f03b528da5a2ffc0189b26853266f6ba2489c69` |
| 4 (isolate) | 0 | `66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925` | `6841360c725e8cfcad4fab032784e071ee0bbf981e301e3982780c9661c34094` |

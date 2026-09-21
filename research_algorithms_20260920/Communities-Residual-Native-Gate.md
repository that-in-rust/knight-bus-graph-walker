# Native Gate For Residual-Degree Quotient Envelopes

Date: 2026-09-21. Protocol frozen before the first native run. This is a
semantic usefulness study, not a physical-memory or timing benchmark.

## Question And Decision

Does retaining residual degrees improve useful native community decisions
beyond weighted Misra-Gries underestimates with original modularity degrees?
The latter is a mandatory strong control: a retained-edge lower objective can
already certify non-worsening coarsenings without the new envelope.

Positive sketch performance is not automatically positive envelope performance.
The gate will distinguish retained-edge quality, added lower-bound information,
upper-bound sharpness, selected-partition changes, and refused quality claims.

## Frozen Inputs And Schedule

- Cached SNAP GrQc and Facebook inputs and the cached NetworkX 3.6.1 Karate
  fixture, with hashes checked against the existing native source receipt.
- Karate both unit and supplied weighted projections; GrQc/Facebook simple
  unit undirected normalized projections, preserving the prior normalization.
- Resolution gamma in {1/2, 1, 3/2}.
- Base partition P0: one exact ascending-ID modularity move sweep from
  singleton labels, with smallest numeric label breaking score ties and only
  strictly positive moves accepted. A fresh singleton is a candidate. This is
  a declared standard heuristic, not a supplied final optimizer answer.
- Canonicalize occupied base labels to contiguous IDs. The study materializes
  the exact quotient only for the oracle comparator and budget calibration.
- Non-loop counter budgets b in {0, ceil(q/100), ceil(q/20), ceil(q/5),
  ceil(q/2), q}, deduplicated, where q is the exact number of non-loop coarse
  pairs. q-based calibration is oracle information, not an implemented
  production estimator. A production user would supply a counter budget.
- Feed once-per-original-undirected-edge coarse contributions in ascending
  original-ID pair order and its reverse. Do not preaggregate the sketch
  stream. Both orders are retained; neither is selected after seeing results.

## Candidate Policy

Each summary generates only these operational candidates:

1. Baseline P0 (singleton coarse labels).
2. Greedy disjoint pair merges sorted by strictly positive retained-edge gain,
   using original degrees and original total weight; numeric pair tie-break.
3. One ascending coarse-ID local-move sweep on retained adjacency, again
   using original degrees and total weight, with fresh singleton candidates.

The candidate producer may inspect the retained summary and original degree
array, not the omitted original edges. Select the operational winner by the
largest certified lower bound, preserving baseline on ties. Compare that
selection with the retained-edge-only lower objective on the same candidates.

For an explicitly separate eligibility/oracle comparison, generate the same
matching and single-sweep candidates from the exact quotient. Score these
under each sketch too, but do not include them in the operational winner.
The best exact score in the resulting five-candidate family is only a finite
candidate comparator, not a global modularity optimum or GDS equivalence.

## Measurements And Controls

For every candidate retain its digest, original exact modularity, known-only
lower score, mass-only interval, independent-group interval, residual-degree
interval, and widths. Mass-only uses [0,M] for residual internal weight with
the exact all-singleton/all-in-one boundaries. Independent-group upper is
sum_C min(s_C/2, s_C-a_C), with the same degree-derived global lower bound L.
The new coupled upper additionally enforces intercommunity degree feasibility.

Record original vertices/edges, K, q, base score, requested/peak/final counters,
retained and residual mass, group count, selected candidate, actual gain,
certified gain, and regret upper bound within the three operational candidates.
Use absolute additive modularity widths {0.001, 0.01, 0.05} as descriptive
sharpness gates, not accepted customer tolerances or multiplicative ratios.

Compare with the exact quotient's two candidates and q counters. An actual
external-sort quotient builder and actual cut sparsifier are not implemented
by this study. Report them as pending comparators, not measured losers.
The known all-cuts route with original normalization has a uniform additive
epsilon guarantee; any future claim must compare full construction/storage
cost at matched output quality with that route as well.

## Accounting And Falsifiers

This driver holds the source graph, initial labels, exact quotient, summaries,
candidate labels and receipt rows in resident Python objects. It is deliberately
an eligibility probe, not a bounded-memory end-to-end implementation. The
summary builder's O(K+b) logical state does not account for arbitrary original
ID mapping, constructing P0, rational bit lengths, publication, or expansion.

Stop promotion if small budgets produce near-baseline candidates, wide
intervals, or no incremental degree-envelope benefit. Retain skew/order
sensitivity and exact-budget controls. Do not replace a failed native gate with
more synthetic stars, which were already sparse before sketching.

## Execution Evidence

Pending the first run under this frozen protocol. Results will be appended
below; changes to the protocol require a separately identified version.

# G02 Metadata Screening Report

**Goal:** G02 - discover metadata candidates
**North star:** `docs_PRD04/A007-spc-founder-interview-prep-v7.md`
**Manifest snapshot:** 262 canonical `METADATA_ONLY` rows
**Manifest SHA-256:** `b96d9ad95ebd75293db8f219ed1fafaab2393c0980bc95c3914801c8941b7b9e`
**Epistemic boundary:** Every interpretation below is a metadata-screening
judgment based on titles, abstracts, comments, and bibliographic metadata. These
are metadata-screening judgments, not paper findings and not `SOURCE_CLAIM`s.

## Executive Result

G02 produced a useful reading map, not an architecture decision. The 25 frozen
query families yielded 531 valid result observations and 262 canonical paper
identities. Four read-only screening lanes covered the manifest exactly once:
Lane A 78, Lane B 128, Lane C 25, and Lane D 31.

The metadata changes the next research priority in five ways:

1. Semi-external and out-of-core BFS/PageRank layouts are the strongest direct
   evidence branch for the first bounded OLAP slice.
2. Algorithm-shaped formats exist across BFS, PageRank, triangles, communities,
   and ANN, but no discovered metadata joins them to whole-process RSS
   enforcement, admission, spill, refusal, and receipts.
3. Disk-resident ANN has a comparatively mature storage-layout design space,
   but it cannot stand in for exact GDS NodeSimilarity semantics.
4. Compatibility coverage is nominal rather than real: regular-path-query
   semantics appear, while Bolt, Cypher, Neo4j GDS modes, driver backpressure,
   and procedure conformance are absent from the metadata.
5. Lower bounds and adverse workload sensitivity deserve equal standing with
   constructive systems papers because they can falsify optimistic memory,
   reuse, latency, and approximation assumptions.

**Decision impact:** `SEED_SET_REPRIORITIZED`. G02 narrows G03 to 25 deliberately
diverse seeds, but it does not select a Knight Bus storage architecture or prove
any RAM or latency claim.

## Campaign Accounting

| Measure | Result |
|---|---:|
| Frozen query families | 25 |
| Query families executed | 25 |
| Query families failed | 0 |
| Query families rate-limited | 0 |
| Query families with no valid result | 0 |
| Logical query variants planned | 125 |
| Valid completed variants | 75 |
| Valid empty variants | 27 |
| External HTTP requests | 191 |
| Cache hits | 0 |
| Retries | 0 |
| Rate-limit events | 0 |
| HTTP 401/403 responses | 0 |
| Raw valid metadata observations | 531 |
| Canonical paper identities | 262 |
| Duplicate observations collapsed | 269 |
| Ambiguous identities | 0 |
| Metadata-only candidates | 262 |
| PDFs or full-text files acquired | 0 |

All 191 requests returned HTTP 200. The request ledger also retains 116 rows
with terminal state `FAILED`: 16 overconstrained compiler requests and 100
requests generated with a faulty date-filter ordering. These are controller-
invalidated responses, not service failures, and none contributes a manifest
candidate. The valid corpus uses exactly one `ALL`, one `PRE2001`, and one
`2001_2010` variant for each family.

The request cap prevented refetching corrected explicit `2011_2020` and
`2021_CURRENT` variants. The valid broad `ALL` pass still returned modern
records, but the missing modern bucket passes reduce recall confidence. This is
reported as a limitation rather than repaired by exceeding the cap.

The only external service used was arXiv. Policy was checked on 2026-08-11 at
`https://info.arxiv.org/help/api/tou.html` and
`https://info.arxiv.org/help/api/user-manual.html`. Crossref and OpenAlex were
not used because deterministic identity reconciliation did not require them.

## Screening Lanes

Candidates are assigned to the lane containing their smallest numeric discovery
query ID. This creates a disjoint audit partition; it is not a semantic claim
that a multi-query paper belongs to only one topic.

| Lane | Query ownership | Candidates | Primary signal | Primary warning |
|---|---|---:|---|---|
| A | QRY-001 through QRY-006 | 78 | PageRank/BFS layouts, preprocessing, external memory | QRY-005 has substantial BFS lexical noise |
| B | QRY-007 through QRY-014 | 128 | WCC, triangles, communities, similarity, disk ANN | ANN dominates scores; NodeSimilarity has no credible direct hit |
| C | QRY-015 through QRY-020 | 25 | bounded RAM, scheduling, compression, hardware | no whole-process RAM or exact spill contract |
| D | QRY-021 through QRY-025 | 31 | path semantics, correctness, compatibility vocabulary | AQ-011 coverage is mostly lexical and contains no Bolt/Cypher/GDS hit |
| **Total** | **QRY-001 through QRY-025** | **262** | **complete disjoint screening** | **metadata only** |

## Coverage By Architecture Question

The counts below are provenance counts inherited from discovery queries. They
measure retrieval coverage, not demonstrated substantive evidence.

| AQ | Candidates | Top lexical metadata candidates |
|---|---:|---|
| AQ-001 | 26 | `PAPER-1709.07122` (31), `PAPER-2203.09284` (28), `PAPER-1809.01415` (14) |
| AQ-002 | 55 | `PAPER-2511.07886` (54), `PAPER-2309.06865` (34), `PAPER-1709.07122` (31) |
| AQ-003 | 93 | `PAPER-2511.07886` (54), `PAPER-2603.04583` (54), `PAPER-1905.04264` (44) |
| AQ-004 | 52 | `PAPER-2511.07886` (54), `PAPER-2112.00098` (26), `PAPER-2406.06754` (26) |
| AQ-005 | 28 | `PAPER-2403.02997` (24), `PAPER-2505.04269` (23), `PAPER-2508.19057` (23) |
| AQ-006 | 25 | `PAPER-1304.4453` (31), `PAPER-2608.07903` (29), `PAPER-1806.08895` (27) |
| AQ-007 | 36 | `PAPER-2602.21600` (49), `PAPER-2602.21514` (46), `PAPER-2603.01779` (42) |
| AQ-008 | 164 | `PAPER-2511.07886` (54), `PAPER-2602.21600` (49), `PAPER-2602.21514` (46) |
| AQ-009 | 34 | `PAPER-1709.07122` (31), `PAPER-2203.09284` (28), `PAPER-LEGACY-7be025519d7c7341` (20) |
| AQ-010 | 80 | `PAPER-2603.04583` (54), `PAPER-2606.05081` (43), `PAPER-2010.09913` (37) |
| AQ-011 | 61 | `PAPER-2511.07886` (54), `PAPER-2606.05081` (43), `PAPER-2607.17269` (40) |
| AQ-012 | 40 | `PAPER-1709.07122` (31), `PAPER-2203.09284` (28), `PAPER-1603.01876` (23) |

The top-score column must not be treated as the reading order. For example,
working-memory BFS, streaming lower bounds, dynamic PageRank lower bounds, set
intersection, RPQ semantics, and explicit error guarantees score poorly because
the lexical score rewards implementation and benchmark words.

## Exploration Quotas

| Quota | Observed coverage | Closure judgment |
|---|---:|---|
| Published through 2000 | 1 | Unfilled in substance: the sole 1998 candidate is a likely query collision |
| Published 2001-2010 | 62 | Useful fallback history, including semi-streaming and external BFS leads |
| Published 2011-2020 | 85 | Strongest source of mature graph-system and algorithm papers |
| Published 2021-current | 114 | Strong modern systems/ANN coverage, with recency bias risk |
| Citation-unknown | 262 | Citation counts were not acquired; no candidate may be called low-citation |
| Neighboring-domain flag | 29 | Available but lexically noisy; manual screening overrides the flag |
| Contradictory-looking | 26 | Explicitly preserved below through lower bounds, failure reports, and applicability limits |
| Pre-arXiv ancestry explicitly identified | 0 | G03 must resolve metadata leads without inventing ancestry |

Contradictory-looking candidates include dynamic PageRank lower bounds,
random-order connected-components lower bounds, Louvain's disconnected-community
failure, HNSW recall sensitivity, ANN applicability boundaries, exact answers
with stochastic runtime, and constant-space BFS with extreme time cost. These
are retained because a falsifier can change an architecture decision even when
it has a low implementation score.

The manifest's neighboring-domain flag is only a lexical heuristic. It marks
some ordinary database or information-retrieval papers while missing genuinely
distant storage systems. It therefore supports exploration but not a product
claim or automatic quota decision.

### Explicit Contradictory-Looking Set

These 26 identities are preserved for adversarial G03 traversal. Inclusion is a
metadata-screening judgment that the record may expose a lower bound, failed
assumption, adverse workload, semantic mismatch, approximation boundary, or
resource trade-off. It is not a claim that the paper proves a contradiction.

| Lane | Candidate IDs |
|---|---|
| A | `PAPER-1204.5500`, `PAPER-2404.16267`, `PAPER-0810.5263`, `PAPER-0802.2847`, `PAPER-2203.09284`, `PAPER-1502.04281`, `PAPER-0709.2016` |
| B | `PAPER-1810.08473`, `PAPER-2402.11454`, `PAPER-2405.17813`, `PAPER-2605.02171`, `PAPER-0906.0684`, `PAPER-1808.06705`, `PAPER-0904.3761` |
| C | `PAPER-2505.06596`, `PAPER-2305.11053`, `PAPER-2209.11889`, `PAPER-2503.00430`, `PAPER-1708.07271` |
| D | `PAPER-1606.05473`, `PAPER-1802.09478`, `PAPER-1709.04290`, `PAPER-2011.08054`, `PAPER-2204.11137`, `PAPER-2412.07729`, `PAPER-2607.17269` |

## Ranking Limitations

1. Scores are deterministic lexical estimates, not relevance labels validated
   by reading papers.
2. Query-derived AQ links overstate coverage when one result matched several
   broad families.
3. ANN terminology produces high resource and implementation scores, while
   exact graph NodeSimilarity remains uncovered.
4. The score underweights theoretical lower bounds, explicit semantics, and
   negative results.
5. `deterministic` often means distributed-protocol behavior rather than bounded
   RAM or predictable latency.
6. Hardware accelerators and distributed systems are overrepresented relative
   to the CPU-local, container-bounded A007 wedge.
7. Metadata contains zero direct references to Neo4j, Cypher, Bolt, GDS
   procedures, projections, result modes, driver conformance, backpressure, or
   demand-driven `PULL`.
8. The apparent AQ-011 and AQ-012 counts therefore do not establish compatibility
   or receipt-verification coverage.

## Recommended G03 Seed Set

The set is capped at 25 and intentionally balances constructive mechanisms,
algorithm families, historical fallbacks, compatibility semantics, and
falsifiers. Order is reading priority, not evidence strength.

| # | Seed | Why it advances an A007 decision |
|---:|---|---|
| 1 | `PAPER-2511.07886` | Out-of-core block scheduling, I/O amplification, and asynchronous execution |
| 2 | `PAPER-1905.04264` | Selective partition loading from active vertices |
| 3 | `PAPER-2507.12925` | Semi-external BFS with disk-resident topology |
| 4 | `PAPER-2010.09913` | BFS-specific vectorizable representation and storage reduction |
| 5 | `PAPER-1709.07122` | Partition-centric PageRank layout and locality |
| 6 | `PAPER-1602.02864` | Sparse topology on SSD with dense state in RAM |
| 7 | `PAPER-1812.10950` | Quantified BFS working-memory floor |
| 8 | `PAPER-2404.16267` | Dynamic PageRank lower bounds and reuse falsification |
| 9 | `PAPER-2602.21514` | Disk-ANN layout, caching, page-read, and search-policy design space |
| 10 | `PAPER-2603.01779` | Comparative disk-resident ANN storage and execution evaluation |
| 11 | `PAPER-2605.02171` | Compressed ANN topology with an applicability boundary |
| 12 | `PAPER-2112.00098` | Finite-memory connectivity over unbounded streams |
| 13 | `PAPER-0708.4284` | Semi-streaming connectivity with an explicit memory model |
| 14 | `PAPER-1810.08473` | Community correctness counterexample and repaired semantics |
| 15 | `PAPER-1304.4453` | Large-scale community implementation comparison |
| 16 | `PAPER-1407.6755` | Set-intersection time/space trade-off tied to triangles |
| 17 | `PAPER-1708.07271` | Direct computation over compressed graph representation |
| 18 | `PAPER-2104.09616` | Memory, latency, and precision co-design for PPR |
| 19 | `PAPER-2012.10026` | Graph reordering, locality, and BFS preparation economics |
| 20 | `PAPER-2503.00430` | Graph-shape and hardware sensitivity of BFS optimization |
| 21 | `PAPER-2305.11053` | Streaming connected-components space lower bound |
| 22 | `PAPER-2204.11137` | Regular-path-query all-shortest semantics and compact paths |
| 23 | `PAPER-2412.07729` | Output-sensitive regular-path-query evaluation |
| 24 | `PAPER-1603.01876` | Whole-pipeline PageRank benchmark vocabulary |
| 25 | `PAPER-2401.01019` | Explicit absolute-error contract for approximate PPR |

G03 should traverse references and later implementation/evaluation branches from
these seeds, stopping when an ancestor or descendant cannot change an open AQ.
This paragraph is a handoff only; no citation edge was created in G02.

## Unresolved Coverage Gaps

- No candidate joins algorithm-specific storage to cgroup enforcement,
  whole-process peak RSS, calibrated overshoot, spill admission, and refusal.
- No credible exact GDS-style NodeSimilarity/incidence-postings candidate.
- No clear pair-once external union-find or Afforest implementation candidate.
- No community candidate exposes bounded neighbor-community maps, spill
  amplification, and contraction storage together.
- No direct destination-major pull/CSC PageRank paper under a strict RAM ceiling.
- No strong direction-optimizing BFS result or bounded predecessor/output-state
  treatment for proof paths.
- No literature spine for Bolt protocol behavior, Cypher eligibility/errors,
  GDS projection ownership, procedure modes, or driver-visible backpressure.
- No receipt schema, independent oracle, differential/metamorphic suite, or
  resource reconciliation method appears directly in the metadata.
- No useful published-through-2000 seed was recovered. Three metadata leads to
  1990s algorithms remain unverified and belong to G03 citation archaeology.
- Modern date-bucket recall is incomplete because the corrected bucket requests
  would have exceeded the 200-request cap.

## Adversarial Review

A final read-only gpt-5.6-sol xhigh reviewer independently recomputed all
campaign arithmetic, the canonical identity set, all 25 seeds, AQ and era
coverage, the four disjoint lanes, the request ledger, and the prohibited-
artifact boundary. It confirmed the arithmetic and found five structural
defects, all repaired before closure:

1. stale lifecycle counts and an incomplete G01 snapshot reconstruction;
2. request-cap enforcement around retries and successful retry semantics;
3. missing byte-level cache and aggregate checksum verification;
4. insufficient seed and contradictory-quota auditability;
5. an ignored-cache escape for unreferenced PDF/archive/full-text files.

Post-repair tests require exactly 25 unique `METADATA_ONLY` seeds, an 8/8/5/4
lane balance, all 12 AQs, 26 explicit contradictory-looking identities, exact
cache bytes and Atom counts, date-valid completed variants, recomputed query
aggregates, and a closed cache-file allowlist.

Residual limitations remain: policy assertions were not re-browsed by the
read-only reviewer; request timestamps have only whole-second resolution; and
the ignored response bodies must remain available locally to reproduce the
abstract/comment screening context.

## Scope Boundary

G02 remains a metadata-discovery goal. It created no PDF, source archive,
full-text cache, citation edge, evidence card, mechanism card, failure card,
constraint-transfer card, architecture candidate, experiment, or GitHub clone.
Every manifest row remains `METADATA_ONLY` with `local_path=NOT_ACQUIRED` and
`sha256=NOT_ACQUIRED`. G03 remains `NOT_STARTED`.

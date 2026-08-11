# Arxiv Pattern Foundry Campaign Status

- Active goal: `G03`
- Goal state: `COMPLETE`
- G02 state: `COMPLETE_VERIFIED`
- Implementation statement: The reproducible two-provider campaign resolved all 25 seeds, retained 115 new identities and 158 provider-backed citation pairs, attempted one depth-2 neighborhood, durably screened all 137 retained depth-1 identities, and produced an exact 50-paper G04 queue without reading or acquiring a paper.
- Research state: `CITATION_ANCESTRY_COMPLETE`
- Completion state: `COMPLETE`
- Validation state: `VERIFIED`
- Scope cap: exactly 25 seeds, citation depth 2, 250 new canonical identities, 90 HTTP attempts, and 6,000 raw metadata observations
- Journal: `arxiv-reference/journals/G03-progress.md`

G01 remains complete and verified at 12 open architecture questions, 109
traceable taxonomy terms, and 25 planned query families at G01 closure. Those
families were frozen before G02. G02 changed every query row from `PLANNED` to
`EXECUTED` without adding a family or expanding its meaning beyond syntax
translation and date constraints.

G02 queried only the authorized arXiv metadata API. Crossref and OpenAlex were
not used. All paper records remain metadata-only. The local response cache is
ignored by Git, but every cache body is ledger-referenced and verified against
its SHA-256, Atom record count, date bucket, and aggregate query checksum.

## Preserved G01 Discovery Artifact Counts

| Measure | Count |
|---|---:|
| Architecture questions | 12 |
| Taxonomy terms | 109 |
| Planned query families at G01 closure | 25 |

## Exact Research Artifact Counts

| Measure | Count |
|---|---:|
| Query families executed | 25 |
| Query families failed | 0 |
| Query families rate-limited | 0 |
| Query families empty | 0 |
| Valid completed variants | 75 |
| Valid empty variants | 27 |
| Controller-invalidated request rows | 116 |
| External HTTP requests | 191 |
| Cache hits | 0 |
| Retries | 0 |
| Raw metadata records | 531 |
| Canonical paper records | 262 |
| Duplicate observations collapsed | 269 |
| Ambiguous identities | 0 |
| Papers screened | 262 |
| Papers read | 0 |
| Full-text files downloaded | 0 |
| Citation edges | 0 |
| Mechanism cards | 0 |
| Failure cards | 0 |
| Constraint-transfer cards | 0 |
| Evidence conflicts | 0 |
| Architecture genomes | 0 |
| Architecture candidates | 0 |
| Candidates changed | 0 |
| Experiments created | 0 |

The 116 failed rows are preserved provenance for 16 overconstrained compiler
attempts and 100 faulty date-filter attempts. All returned HTTP 200, but none
contributes to the 531 valid observations or 262 canonical identities.

## Coverage Snapshot

| Measure | Count |
|---|---:|
| Published through 2000 | 1 |
| Published 2001-2010 | 62 |
| Published 2011-2020 | 85 |
| Published 2021-current | 114 |
| Citation-unknown candidates | 262 |
| Neighboring-domain flagged candidates | 29 |
| Explicit contradictory-looking candidates | 26 |
| Recommended G03 seeds | 25 |

The four disjoint screening lanes covered all canonical candidates exactly once:
A 78, B 128, C 25, and D 31. The final 25 seeds balance those lanes 8/8/5/4,
cover all 12 architecture questions, and remain `METADATA_ONLY`.

## Decision Yield

- A007 uncertainty reduced: which evidence branches are most likely to change
  the first bounded OLAP architecture and which gaps remain unsupported?
- Decision impact: `SEED_SET_REPRIORITIZED`
- Result: semi-external BFS/PageRank layouts, compressed computation, bounded
  connectivity, algorithm-specific ANN storage, explicit path semantics, and
  lower-bound falsifiers now form the G03 reading spine.
- Non-result: no storage architecture, RAM improvement, latency improvement,
  compatibility promise, or correctness claim has been proved.

The strongest unresolved gap is still A007's paid promise: no discovered
metadata joins algorithm-specific storage with whole-process RSS enforcement,
calibrated admission, spill/refusal behavior, correctness verification, and a
machine-checkable receipt. Compatibility coverage also lacks direct Bolt,
Cypher, and Neo4j GDS procedure evidence.

## Adversarial Review

The independent read-only `gpt-5.6-sol` xhigh reviewer initially returned
`NOT_CLEARED` after recomputing the campaign. Arithmetic passed, but six P1 and
two P2 contract defects prevented premature closure.

The repair pass has:

1. enforced new-identity quotas after reconciliation and global deduplication;
2. persisted all 1,251 exact stopped observations rather than aggregates;
3. replaced the hardcoded handoff with a checksummed 137-row screening ledger;
4. preserved conflicting exact arXiv/DOI anchors as separate ambiguous IDs;
5. strengthened completion validation to require the exact 50-paper queue;
6. repaired stale index, lifecycle, journal, and schema ownership records;
7. disclosed the separately user-authorized commit and push; and
8. made the lane documents the reproducible source of screening decisions.

The same reviewer then found and drove three deeper repairs: the report queue is
now bound to exact screening ranks, strong-identifier conflict anchors survive
into final manifest notes, and every control stop preserves available provider
and AQ provenance. Its final verdict was `CLEARED` with no P0, P1, or P2
findings. The 96-test suite, full corpus validator, independent accounting,
Git/license gates, and network-disabled six-artifact byte replay all pass.

Permissible coverage limits remain explicit: one rate-limited forward branch,
one rejected depth-2 payload, zero retained depth-2 identities, provider-visible
bibliography incompleteness, and one-page provider limits.

## G03 Verified Completion

| Measure | Result | Cap |
|---|---:|---:|
| Initial seeds | 25 | exactly 25 |
| HTTP attempts | 83 | 90 |
| Selected metadata observations | 1,389 | 6,000 |
| Baseline identities | 262 | frozen |
| Final identities | 377 | N/A |
| New identities | 115 | 250 |
| Retained depth-1 identities screened | 137 | 137 |
| Exact stopped observations | 1,251 | N/A |
| Provider-backed `CITES` edges | 158 | N/A |
| Metadata-inferred `IMPLEMENTS` edges | 1 | N/A |
| Retained depth-2 identities | 0 | N/A |
| G04 queue | 50 | 50 |
| Papers read or acquired | 0 | 0 |

OpenAlex contributed 28 requests and one selected observation but no retained
edge. Exact Semantic Scholar resolution covered all 25 seeds; its 55 attempts
contributed 1,388 observations and all 158 retained citation pairs. One forward
branch exhausted three 429 attempts. One depth-2 neighborhood returned an
HTTP-success envelope that violated the selected-metadata contract and is
preserved as `PAYLOAD_REJECTED` without retaining its raw body.

Three disjoint read-only `gpt-5.6-sol` xhigh screening lanes covered 66 backward,
57 forward, and 14 constraint/survey identities; a fourth lane audited
provenance and accounting. They
replaced generic false positives and ambiguous duplicate identities in the G04
queue with external-memory traversal, graph-shaped storage, compression,
algorithm-operable representations, named implementations, and explicit survey
or failure-boundary candidates. Only the CUDA implementation of the named
PageRank Pipeline Benchmark satisfied the strict title-token and target-anchor
rule for an `IMPLEMENTS` inference; every other role remains `CITES` only.

G03 downloaded no PDF, abstract, paper body, source
archive, or repository; read no paper; created no mechanism, failure, or
transfer card; and proposed no architecture or experiment. Commit `327a68c`
and its push were separately authorized by the user's explicit commit-and-push
instruction. No later commit or push has been authorized or performed.

## Exact Closure Counts

| Measure | Count |
|---|---:|
| External citation HTTP attempts | 83 |
| Citation metadata observations | 1389 |
| Final canonical paper records | 377 |
| New G03 canonical identities | 115 |
| Citation and semantic edges | 159 |

- Recommended next goal: `G04`
- G04 state: `NOT_STARTED`
- Final reviewer: `019ff0c1-4af5-74c0-876d-d67f5b7437aa`

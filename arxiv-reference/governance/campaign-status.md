# Arxiv Pattern Foundry Campaign Status

- Active goal: `G03`
- Goal state: `IN_PROGRESS`
- G02 state: `COMPLETE_VERIFIED`
- Implementation statement: The reproducible 28-request OpenAlex pass resolved 1 of 25 seeds and produced zero edges. G03 is in offline RED for an exact-arXiv Semantic Scholar recovery adapter under the unchanged cap.
- Research state: `SEMANTIC_SCHOLAR_PROVIDER_RECOVERY_RED`
- Completion state: `IN_PROGRESS`
- Validation state: `RED_PROVIDER_ADAPTER_PENDING`
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

A final read-only gpt-5.6-sol xhigh reviewer independently recomputed the
request, variant, raw, canonical, duplicate, era, AQ, neighboring-domain, lane,
and seed counts. It found no arithmetic, identity, ranking-order, seed,
SOURCE_CLAIM, or actual G03-boundary defect.

Five structural findings were repaired before closure:

1. stale campaign status and G01 snapshot reconstruction;
2. per-attempt request-cap and successful-retry semantics;
3. byte-level cache, result-count, date, and aggregate-checksum verification;
4. exact seed and contradictory-quota auditability;
5. rejection of unreferenced or full-text files hidden under the ignored cache.

Residual limitations are explicit: modern date-bucket recall is incomplete,
the single pre-2001 result is not useful, citation counts are unknown, and the
ignored response cache must be retained locally to replay metadata screening.

## Active G03 Boundary

- G03 state: `IN_PROGRESS`
- G03 cap: exactly 25 seeds, citation depth 2, and at most 250 new identities

G03 may traverse backward to foundational terminology and forward to
implementations, evaluations, refinements, and contradictions under the frozen
Goal Packet, service preflight, fixtures, tests, and journal. At this checkpoint,
G03 has made 28 exact OpenAlex metadata requests, retained one raw observation,
and created zero citation edges. Exact provider-unavailable seeds remain known G02
identities and stop only their own branches. G02 did not
download full text, read a paper, create an evidence card, synthesize an
architecture, or design an experiment.

The next permitted operation is the offline-tested Semantic Scholar adapter and
then one exact 25-seed batch request. G04 remains forbidden until G03 records
real citation branches or an explicit provider-coverage failure.

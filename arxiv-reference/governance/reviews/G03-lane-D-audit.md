# G03 Lane D Provenance And Accounting Audit

- Lane ID: `G03-LANE-D`
- Agent ID: `019ff0d9-98c8-7221-ac35-561d3cbe24fe`
- Model: `gpt-5.6-sol`
- Reasoning effort: `xhigh`
- Completed: `2026-08-11T12:54:47Z`
- Evidence scope: `COMMITTED_METADATA_AND_CONTROLS_ONLY`
- Recommendations: 0
- Boundary: read-only, offline, no ignored cache, abstract, PDF, paper body, repository, or file edit

## Reconciliation At Audit Time

| Surface | Recomputed result |
|---|---:|
| Seeds | 25 |
| Requests | 83 |
| Selected metadata observations | 1,389 |
| Baseline / final / new identities | 262 / 377 / 115 |
| Retained depth-1 identities | 137, including 22 rediscovered baseline identities |
| New-identity quota violations | 0; maximum 3 per seed and direction |
| CITES / IMPLEMENTS edges | 158 / 1 |
| Exact stopped observations | 1,251 |
| Depth-2 attempts / retained identities | 1 rejected / 0 |

## Findings Returned For Integration

1. Campaign status still carried pre-repair identity and edge counts.
2. The screening ledger and lane-result checksums had not yet been persisted.
3. Screening coverage must include all 137 retained depth-1 identities, while
   the G04 ancestry queue must contain 25 new identities only.
4. The fallback report queue still contained six explicitly rejected or
   ambiguous identities and could not be retained.
5. Campaign lifecycle markers had to remain `IN_PROGRESS` and `NOT_CLEARED`
   until these findings and the independent final review were resolved.

The audit passed seed equality, request and observation caps, repaired
per-seed quotas, citation orientation, semantic-edge companionship, metadata-
only manifest state, and the no-acquisition boundary. It classified the one
rate-limited branch, one rejected depth-2 payload, zero retained depth-2
identities, and one-page recall limits as permissible G03 coverage gaps.

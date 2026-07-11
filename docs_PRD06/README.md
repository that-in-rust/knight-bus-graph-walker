# PRD06: The AI-Regime Rewrite Thesis

## Status

`docs_PRD06/` captures the analysis that emerged from a live rewrite-sampling
experiment (2026-07-08) and the discussion that followed it. It updates — and
in one important place inverts — the sequencing conclusions of `docs_PRD05/`.

It does not overwrite PRD05's claims; it re-derives the plan under one changed
assumption: code generation is now nearly free, so the cost model that
produced "one algorithm in 90 days" no longer describes the real bottleneck.

## Contents

| File | What it contains |
| --- | --- |
| `Rewrite-Sampling-And-Convergence-Thesis.md` | The 5-minute sampling experiment, the AI-regime effort re-estimation, the known-endpoint convergence thesis and its three conditions, and the harness-first program design |

## One-Paragraph Summary

A five-minute sampling run produced two spec-tested crates of a Neo4j-parallel
Rust workspace, demonstrating that typing volume is no longer the cost driver
of a rewrite. With stock Neo4j available as an *executable specification*, a
rewrite becomes a search problem: generate → differential-test → feed failures
back → regenerate. That loop converges mechanically for roughly 80% of the
surface. The residual human work concentrates in exactly three places —
observability harnesses, test-signal coverage, and equivalence definitions —
and therefore the differential harness, not the Rust code, is the critical
path. The generated code is a regenerable artifact; the harness is the asset.

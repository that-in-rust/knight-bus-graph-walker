# PRD05: Verification Spine Working Set

<!-- markdownlint-disable MD060 -->

## Status

`docs_PRD05/` is the active home for the Neo4j rewrite's reference spine,
verification spine, and new research documents until that direction changes.

The earlier architecture and evidence corpus remains in `docs_PRD04/`. PRD05
should build on that corpus without silently rewriting its historical claims.

## Current Documents

| Document | Role |
|---|---|
| [Sol-01.md](Sol-01.md) | Architecture options and the 90-day, verification-first execution strategy |
| [Sol-02.md](Sol-02.md) | Stepwise research conclusions, beginning with the graph-algorithm Pareto question |
| [Neo4j-Rust-Rewrite-Feasibility.md](Neo4j-Rust-Rewrite-Feasibility.md) | Quantitative feasibility analysis for a Neo4j-compatible Rust rewrite |
| [Neo4j-Rust-Two-Scenario-Estimation.md](Neo4j-Rust-Two-Scenario-Estimation.md) | Two-draft quantitative comparison of a resident faithful rewrite and the read-shape architecture |
| [Neo4j-Rust-Prior-Art-Feasibility-Risk-Assessment.md](Neo4j-Rust-Prior-Art-Feasibility-Risk-Assessment.md) | Primary-source prior art, feasibility boundaries, closest systems, research risks, and falsification plan |

## Working Boundary

- Put new verification-spine and rewrite-planning notes in this folder.
- Treat `docs_PRD04/` as the preceding architecture/reference corpus.
- Preserve source URLs, local evidence paths, measurements, assumptions, and
  uncertainty labels so claims can become executable tests later.
- Prefer one durable document per research question, with links back to the
  governing PRD and the exact code or test evidence used.

## Intended Outcome

PRD05 should converge from research into a runnable verification system:

1. a compatibility contract for the supported Neo4j surface;
2. an oracle and differential-test harness;
3. deterministic fixtures and expected outputs;
4. RAM and latency benchmark gates;
5. a traceability map from requirement to evidence, test, implementation, and
   benchmark result.

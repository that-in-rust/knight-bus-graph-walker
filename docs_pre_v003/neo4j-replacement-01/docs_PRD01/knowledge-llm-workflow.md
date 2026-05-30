# LLM Workflow: Work Type Classification (from agent-room-of-requirements A01)

## Quick Classifier

| Work Type | Use it for | PRD | Architecture | TDD | Time |
|---|---|---|---|---|---|
| Bug | Something broke | None | None | Fix + regression test | Hours |
| Enhancement | Small change on existing rails | Light | Light | Yes | Days |
| Feature | New bounded capability | Yes | Yes | Full | Weeks |
| Product | New system or unclear scope | Deep | Extensive | Full + spikes | Months |

## Decision Tree

1. Is something broken? → Bug Flow
2. Does similar code exist? → Enhancement Flow
3. Is the scope clear? → Feature Flow
4. Otherwise → Product Flow

## Bug Flow (Hours)

Reproduce → Diagnose → Write regression test → Fix → Verify → Update anti-patterns

## Enhancement Flow (Days)

Confirm fits existing rails → Short PRD → Find closest prior example → TDD → Review → Ship

**Rule**: If you can say "do X like we do Y," it's an enhancement. If you can't find Y, it's a feature.

## Feature Flow (Weeks)

PRD v1 → ARCH v1 → PRD v2 (tighten) → ARCH v2 → TDD → Ship

**Key**: Let architecture remove scope. Don't treat first PRD as sacred.

## Product Flow (Months)

Signals → Problem → Sizing → Hypotheses → Feasibility → MVP PRD → MVP ARCH → MVP TDD → Learn → Iterate or pivot

**Key**: Product work is about learning before scale.

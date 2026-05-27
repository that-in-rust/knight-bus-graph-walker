# Product/Feature Development Flow

Use this playbook when building new capabilities or systems.

## Step 1: Classify the Work Type
- Is something broken? → Bug Flow (hours, skip this playbook)
- Small change on existing rails? → Enhancement (days, light PRD)
- New bounded capability, clear scope? → Feature Flow (weeks)
- New system, unclear scope? → Product Flow (months)

## Step 2: Problem Discovery (Product Flow only)
1. Define the problem before the feature list
2. Write key hypotheses
3. Run spikes to test risky assumptions
4. Size the opportunity

## Step 3: PRD (Requirements)
1. Write PRD v1 from the user journey
2. Use executable specifications, not narrative user stories
3. Every requirement must have a testable acceptance criterion
4. PRD should co-evolve with architecture — let architecture simplify scope

## Step 4: Architecture
1. Write ARCH v1 alongside PRD
2. Apply Layered Rust Architecture: Core (L1) → Std (L2) → External (L3)
3. If the design simplifies scope, update the PRD
4. Use Mermaid diagrams only
5. Iterate: PRD v2 ← simpler path found ← ARCH v1

## Step 5: Implementation (TDD)
1. Follow STUB → RED → GREEN → REFACTOR cycle
2. Use Four-Word Naming Convention for all identifiers
3. Write checkpoint summaries after each major decision
4. Apply the reliability patterns from rust-coder-02

## Step 6: Review and Ship
1. Run all quality gates (`cargo test`, `clippy`, `fmt`)
2. Review against original PRD acceptance criteria
3. Document any scope changes and rationale
4. Record failures, fixes, and wins for future reference

## Key Rules
- Over-process wastes time. Under-process builds the wrong thing.
- Let architecture remove scope. Do not treat first PRD as sacred.
- Product work is about learning before scale.
- If you can describe work as "do X like we do Y", it is an enhancement, not a feature.

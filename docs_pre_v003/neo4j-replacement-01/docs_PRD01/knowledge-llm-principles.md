# 12 Principles of LLM-Native Development (from agent-room-of-requirements A00)

1. **LLMs are retrieval systems** — Use precise names, types, constraints, and input context. Fix inputs before blaming the model.
2. **Iteration is required** — First output is a draft. Use 4 passes: Explore → Constrain → Refine → Verify.
3. **Context windows forget** — Long sessions lose earlier decisions. Write summary checkpoints to files.
4. **Rubber duck debugging finds weak spots** — Ask the model to explain, challenge assumptions, and restate the work.
5. **Negative knowledge is leverage** — "Do not do this" removes bad paths fast. Keep anti-patterns and failure notes.
6. **Tests are the spec** — Executable checks beat prose. Write the test before the implementation.
7. **Four-word names are a strong default** — Names shape retrieval quality. Prefer clear, specific, stable names.
8. **Match process to work type** — Bugs and products need different processes. Use the lightest process that protects quality.
9. **PRD and architecture should co-evolve** — Let architecture discoveries remove requirements. Don't treat first PRD as sacred.
10. **Serialize state** — Save phase, tests, decisions, next steps. Progress disappears without checkpoints.
11. **Delegate with rules** — Use explicit routing rules, not vibes.
12. **Close the loop** — Record failures, fixes, and wins. Teams improve only from outcomes.

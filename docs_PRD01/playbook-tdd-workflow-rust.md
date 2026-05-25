# TDD Workflow for Rust (STUB-RED-GREEN-REFACTOR)

Follow the STUB to RED to GREEN to REFACTOR cycle for all implementation work.

## Phase 1: STUB
1. Create the module/file structure
2. Define public API signatures with `todo!()` or `unimplemented!()` bodies
3. Ensure the project compiles (`cargo check`)

## Phase 2: RED (Write Failing Tests)
1. Write tests that describe the desired behavior
2. Use the Four-Word Naming Convention for test functions: `test_verb_constraint_target_qualifier`
3. Run tests — they MUST fail (red)
4. If tests pass without implementation, the tests are wrong

## Phase 3: GREEN (Make Tests Pass)
1. Write the MINIMUM code to make each test pass
2. Do not optimize, do not refactor yet
3. Run tests after each change — aim for all green
4. Commit when all tests pass

## Phase 4: REFACTOR
1. Clean up the implementation while keeping tests green
2. Apply idiomatic Rust patterns (newtypes, combinators, borrowed inputs)
3. Run `cargo clippy -D warnings` and `cargo fmt`
4. Run full test suite again
5. Commit the refactored code

## State Tracking
After each phase, create a checkpoint note:
- Current TDD phase (RED/GREEN/REFACTOR)
- Tests written and their status (passing/failing/pending)
- Implementation decisions and rationale
- Next steps
- Any blockers or questions

## Quality Gates
- `cargo test` — all tests pass
- `cargo clippy -D warnings` — no warnings
- `cargo fmt --check` — properly formatted
- Performance claims backed by benchmark tests
- All diagrams in Mermaid format

## Architecture Rules
- Depend on traits, not concrete types (Dependency Injection)
- `thiserror` for library errors, `anyhow` for application errors
- RAII for all resource management
- Layered architecture: Core (L1) → Std (L2) → External (L3)
